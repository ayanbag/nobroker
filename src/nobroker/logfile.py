"""The append-only write-ahead log: the only durable state nobroker has.

Everything else -- the ready heap, the lease table, the DLQ -- is a cache of
this file. That is the design in one sentence, and it is what makes crash
recovery tractable: there is no second thing to keep consistent with it.

Three rules hold the durability guarantee up:

1. **Append only.** Bytes already written are never modified in place. A crash
   can therefore only damage the *tail*, and only the record being written.
2. **fsync before returning.** ``enqueue()`` returns after the data is on the
   platter, not after it reached the page cache. Anything weaker means "durable"
   is a claim about a machine that did not lose power.
3. **Checksum every record.** Recovery can then tell a complete record from a
   half-written one, which is the only way to know where the log really ends.

Torn tails are *expected*, not exceptional. Crash a process mid-``write`` and
you get one. Recovery truncates back to the last complete record and carries on;
the interrupted operation is simply one that never happened.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

from .codec import (
    HEADER_SIZE,
    Damage,
    Record,
    RecordType,
    decode_file_header,
    decode_record,
    encode_file_header,
    encode_record,
)
from .errors import CorruptLogError

#: Read size when scanning. Large enough that scanning a 100 MB log is a few
#: hundred syscalls, small enough not to balloon memory on a huge log.
_CHUNK = 1 << 20

_WINDOWS = os.name == "nt"

#: Must be OR-ed into every ``os.open`` that writes bytes. On Windows a
#: descriptor opened without it is in *text mode*, and the C runtime silently
#: rewrites every ``0x0A`` in the buffer as ``0x0D 0x0A`` on the way to disk.
#: For a binary log that is data corruption -- and corruption that only appears
#: when a record's checksum or payload happens to contain a newline byte, so it
#: shows up as an intermittent CRC failure long after the write. Zero on POSIX,
#: where the flag does not exist because the problem does not.
O_BINARY = getattr(os, "O_BINARY", 0)

if _WINDOWS:  # pragma: no cover - exercised on Windows only
    import ctypes
    import ctypes.wintypes as wintypes
    import msvcrt

    _GENERIC_READ = 0x80000000
    _GENERIC_WRITE = 0x40000000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_NORMAL = 0x80
    _INVALID_HANDLE = wintypes.HANDLE(-1).value

    _CreateFileW = ctypes.windll.kernel32.CreateFileW
    _CreateFileW.restype = wintypes.HANDLE
    _CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]


def open_log_fd(path: Path) -> int:
    """Open an existing log read-write without pinning it against deletion.

    On POSIX this is :func:`os.open` and there is nothing to say. On Windows an
    ordinary open takes a share mode that forbids anyone deleting or renaming
    the file while the handle lives -- so a peer process merely *having the queue
    open* would block compaction from retiring the old log. POSIX has no such
    rule: unlinking a file other processes have open is normal there.

    Opening the handle ourselves through ``kernel32.CreateFileW`` with
    ``FILE_SHARE_DELETE`` restores the POSIX behaviour, and
    :func:`msvcrt.open_osfhandle` hands it back to the C runtime as a plain file
    descriptor -- ``os.read``, ``os.write``, ``os.fsync`` and ``os.close`` all
    work on it unchanged. Both :mod:`ctypes` and :mod:`msvcrt` are standard
    library, so this is Windows parity bought without a dependency.

    Note that this flag alone is *not* enough to let ``os.replace`` overwrite an
    open file on Windows -- ``MoveFileEx`` refuses regardless. That is why
    compaction writes a new numbered generation and flips a pointer instead of
    overwriting in place; see :class:`Layout`.
    """
    if not _WINDOWS:
        return os.open(path, os.O_RDWR)

    handle = _CreateFileW(  # pragma: no cover - Windows only
        str(path),
        _GENERIC_READ | _GENERIC_WRITE,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        None,
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL,
        None,
    )
    if handle == _INVALID_HANDLE:  # pragma: no cover - Windows only
        raise OSError(ctypes.get_last_error(), f"could not open {path}")
    return msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY)  # pragma: no cover


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """What happened when a log was read back after a crash.

    Surfaced by ``Queue.recovery`` and printed by ``nobroker recover`` so that a
    repaired torn tail is *visible*, not silent. Silent repair is how you find
    out a year later that you have been losing the last job of every crash.
    """

    records_applied: int = 0
    bytes_scanned: int = 0
    bytes_discarded: int = 0
    damage: Damage = Damage.NONE
    orphan_records: int = 0

    @property
    def repaired(self) -> bool:
        """True if a damaged tail was found and truncated."""
        return self.bytes_discarded > 0

    def describe(self) -> str:
        """One human-readable line, for CLI output and logs."""
        if not self.repaired:
            return (
                f"clean: {self.records_applied} records, "
                f"{self.bytes_scanned} bytes"
            )
        return (
            f"repaired: {self.records_applied} records applied, "
            f"{self.bytes_discarded} bytes discarded from the tail "
            f"({self.damage.value})"
        )


class LogFile:
    """A single append-only log file plus the syscalls that keep it honest.

    Deliberately dumb: it knows about bytes, offsets and fsync, and nothing about
    jobs or leases. Queue semantics live in :mod:`nobroker.index`.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._fd: int | None = None
        self._end = 0
        self._generation = 0

    # -- lifecycle --------------------------------------------------------

    def open(self, *, generation: int = 0) -> None:
        """Open the log, creating it with a fresh header if it does not exist.

        Creation is done through a temp file and :func:`os.replace` so that a
        crash between "file exists" and "file has a valid header" is impossible:
        another process sees either no log at all or a complete one.
        """
        if self._fd is not None:
            return
        if not self.path.exists():
            self._create(generation)

        self._fd = open_log_fd(self.path)
        try:
            header = self._read_at(0, HEADER_SIZE)
            self._generation = decode_file_header(header)
            self._end = os.lseek(self._fd, 0, os.SEEK_END)
        except BaseException:
            os.close(self._fd)
            self._fd = None
            raise

    def _create(self, generation: int) -> None:
        """Atomically materialise a new log file with a valid header."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + f".new.{os.getpid()}")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | O_BINARY, 0o644)
        try:
            os.write(fd, encode_file_header(generation))
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.replace(tmp, self.path)
        except OSError:
            # Lost a creation race with a peer process; their file is just as
            # valid as ours would have been.
            tmp.unlink(missing_ok=True)
            if not self.path.exists():
                raise
        fsync_directory(self.path.parent)

    def close(self) -> None:
        """Close the descriptor. Idempotent."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    @property
    def generation(self) -> int:
        """Compaction counter baked into the file header."""
        return self._generation

    @property
    def end_offset(self) -> int:
        """Offset one past the last byte known to be written."""
        return self._end

    # -- writing ----------------------------------------------------------

    def append(
        self,
        records: Sequence[tuple[RecordType, dict[str, Any]]],
        *,
        fsync: bool = True,
    ) -> int:
        """Append records and return the new end offset.

        The batch is encoded, concatenated, and handed to a *single*
        :func:`os.write`. That is what makes ``enqueue_many`` worth having: N
        jobs cost one write and one fsync instead of N of each, and the batch
        keeps its all-or-nothing feel because a torn write is detected and
        discarded wholesale by recovery.

        Args:
            records: ``(type, payload)`` pairs, applied in order.
            fsync: When False the data is handed to the OS but not forced to
                disk, so a machine crash can lose it. Only ever set this from
                :class:`~nobroker.queue.Queue` with ``fsync=False``, which the
                README documents as trading durability for throughput.
        """
        if self._fd is None:
            raise CorruptLogError("log is not open")
        if not records:
            return self._end

        blob = b"".join(encode_record(rtype, payload) for rtype, payload in records)
        os.lseek(self._fd, self._end, os.SEEK_SET)
        written = 0
        while written < len(blob):
            # os.write may write short. Looping is not paranoia; it is the
            # documented contract, and a short write here would corrupt the log.
            written += os.write(self._fd, blob[written:])
        if fsync:
            os.fsync(self._fd)
        self._end += len(blob)
        return self._end

    # -- reading ----------------------------------------------------------

    def scan(self, start: int) -> tuple[list[Record], int, Damage]:
        """Read every complete record from ``start`` to the end of the log.

        Returns ``(records, valid_end, damage)`` where ``valid_end`` is the
        offset one past the last record that decoded cleanly. If ``damage`` is
        not :attr:`Damage.NONE`, everything from ``valid_end`` onward is garbage
        and the caller decides whether to truncate it.
        """
        if self._fd is None:
            raise CorruptLogError("log is not open")

        records: list[Record] = []
        buf = b""
        buf_base = start
        cursor = start
        # Ask the kernel where the file actually ends rather than trusting our
        # own cached offset. A peer process appends to the same file, so a cached
        # end is stale the moment we release the lock -- and appending at a stale
        # offset would overwrite the peer's records. This refresh is what makes
        # multi-process appends safe.
        file_end = os.lseek(self._fd, 0, os.SEEK_END)
        self._end = file_end
        read_pos = start
        damage = Damage.NONE

        while True:
            if read_pos < file_end:
                chunk = self._read_at(read_pos, _CHUNK)
                read_pos += len(chunk)
                buf += chunk
                if not chunk:
                    read_pos = file_end

            drained = False
            while True:
                record, size, why = decode_record(buf, cursor - buf_base)
                if why is Damage.NONE and record is not None:
                    records.append(Record(cursor, record.type, record.payload))
                    cursor += size
                    continue
                if why in (Damage.TRUNCATED_HEADER, Damage.TRUNCATED_BODY):
                    # Might just be a chunk boundary rather than a torn tail.
                    if read_pos < file_end:
                        drained = True
                        break
                    if cursor == file_end:
                        why = Damage.NONE  # exactly at EOF: a clean log
                    damage = why
                    break
                damage = why
                break

            if drained:
                # Drop what we have already decoded, keep the partial record.
                buf = buf[cursor - buf_base :]
                buf_base = cursor
                continue
            break

        return records, cursor, damage

    def truncate(self, offset: int) -> int:
        """Cut the log back to ``offset`` and return the bytes discarded.

        Called only to remove a damaged tail. Truncation is itself durable --
        fsync after -- because a crash during recovery must not leave the log in
        a third state that is neither the original nor the repaired one.
        """
        if self._fd is None:
            raise CorruptLogError("log is not open")
        discarded = max(0, self._end - offset)
        if discarded == 0:
            return 0
        os.ftruncate(self._fd, offset)
        os.fsync(self._fd)
        self._end = offset
        return discarded

    # -- helpers ----------------------------------------------------------

    def _read_at(self, offset: int, size: int) -> bytes:
        assert self._fd is not None
        os.lseek(self._fd, offset, os.SEEK_SET)
        return os.read(self._fd, size)

    def iter_all(self) -> Iterator[Record]:
        """Yield every record in the log. Used by ``nobroker inspect``."""
        records, _, _ = self.scan(HEADER_SIZE)
        yield from records


@dataclass(frozen=True, slots=True)
class Layout:
    """Where a queue's files live, and which log is currently authoritative.

    Compaction cannot overwrite the live log in place. On Windows ``os.replace``
    refuses to touch a file another process has open, which is precisely when
    compaction is worth doing. So the log is *versioned* instead::

        emails.000000.log     <- generation 0, being retired
        emails.000001.log     <- generation 1, the live log
        emails.current        <- one line of text: "1"
        emails.lock           <- the cross-process lock

    ``emails.current`` is the only file that gets replaced, and nobody ever holds
    it open -- it is opened, read, and closed inside one call. Replacing it is
    therefore always allowed, on every platform, and it is a single atomic
    ``os.replace`` of a file small enough to land in one sector.

    The flip is the commit point. A crash before it leaves the old generation
    authoritative and the half-written new one ignorable garbage; a crash after
    it leaves the new generation live and the old one an orphan to sweep up. At
    no instant is a partially compacted log the one that readers follow.

    It also gives peers a cheap way to notice: compare the number in the pointer
    against the generation you have open. No inode comparison, no stat games, no
    platform-specific fallback.
    """

    dir: Path
    name: str

    @property
    def pointer(self) -> Path:
        return self.dir / f"{self.name}.current"

    @property
    def lock(self) -> Path:
        return self.dir / f"{self.name}.lock"

    def log(self, generation: int) -> Path:
        """Path of a specific generation. Zero-padded so ``ls`` sorts usefully."""
        return self.dir / f"{self.name}.{generation:06d}.log"

    def read_generation(self) -> int:
        """Which generation is live. Zero when the queue is brand new.

        A pointer file that exists but is unreadable or non-numeric is treated as
        generation 0 rather than as an error: the pointer is written atomically,
        so garbage there means something outside nobroker interfered, and
        falling back to the first generation at least opens the queue.
        """
        try:
            text = self.pointer.read_text(encoding="ascii").strip()
        except (FileNotFoundError, NotADirectoryError):
            return 0
        try:
            return int(text)
        except ValueError:
            return 0

    def write_generation(self, generation: int) -> None:
        """Atomically publish ``generation`` as the live log. The commit point."""
        tmp = self.dir / f"{self.name}.current.{os.getpid()}"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | O_BINARY, 0o644)
        try:
            os.write(fd, f"{generation}\n".encode("ascii"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.pointer)
        fsync_directory(self.dir)

    def stale_logs(self, keep: int) -> list[Path]:
        """Generation files older than ``keep``, oldest first."""
        found: list[tuple[int, Path]] = []
        for path in self.dir.glob(f"{self.name}.*.log"):
            try:
                generation = int(path.name[len(self.name) + 1 : -len(".log")])
            except ValueError:
                continue
            if generation < keep:
                found.append((generation, path))
        return [path for _, path in sorted(found)]


def fsync_directory(path: str | os.PathLike[str]) -> None:
    """fsync a directory so a rename or creation inside it is durable.

    Renaming a file is not durable until the *directory* entry is synced. Skip
    this and a well-timed power cut can leave a compacted log that exists on disk
    but that no directory entry points at. Windows has no directory handle to
    sync, so this is a documented no-op there.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return  # Windows: cannot open a directory; nothing to sync.
    try:
        os.fsync(fd)
    except OSError:
        pass  # Some filesystems reject fsync on directories; not fatal.
    finally:
        os.close(fd)

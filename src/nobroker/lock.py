"""A cross-process advisory file lock.

Multi-process safety is the whole reason nobroker can replace a broker: several
worker processes, no coordinator. That requires exactly one primitive -- mutual
exclusion that the operating system enforces and that survives a process being
killed. Both POSIX ``flock`` and Windows ``msvcrt.locking`` give that: the kernel
drops the lock when the file descriptor closes, including on ``SIGKILL``.

There is no portable stdlib wrapper over the two, so this module is the shim.
Note what is *not* here: no lock file with a pid in it, no stale-lock timeout, no
"is that pid still alive" heuristic. Those are what you write when your lock is
not kernel-backed, and they are all subtly wrong.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType

from .errors import LockTimeoutError

try:  # POSIX
    import fcntl

    _HAVE_FCNTL = True
except ImportError:  # pragma: no cover - exercised on Windows only
    _HAVE_FCNTL = False

try:  # Windows
    import msvcrt

    _HAVE_MSVCRT = True
except ImportError:  # pragma: no cover - exercised on POSIX only
    _HAVE_MSVCRT = False


#: How long to sleep between attempts on platforms without a blocking-with-
#: -timeout primitive. Short enough to be invisible, long enough not to spin.
_POLL_INTERVAL = 0.005


class FileLock:
    """Exclusive advisory lock held on a dedicated lock file.

    The lock file is separate from the log. Locking the log itself would work on
    POSIX but breaks on Windows, where compaction's ``os.replace`` cannot swap a
    file that anyone has open -- and the lock holder always has it open.

    Not reentrant across threads by design: nobroker takes the lock for the
    duration of one queue operation and never nests, so a recursive acquire would
    mean a bug, and turning that bug into a deadlock is more useful than hiding
    it. Within a process, :class:`~nobroker.queue.Queue` also holds a
    ``threading.Lock``, so threads serialise before they ever reach this.
    """

    def __init__(self, path: str | os.PathLike[str], timeout: float = 10.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._fd: int | None = None
        self._held = False

    def acquire(self, timeout: float | None = None) -> None:
        """Block until the lock is held, or raise :class:`LockTimeoutError`."""
        if self._held:
            raise RuntimeError("FileLock is not reentrant and is already held")

        limit = self.timeout if timeout is None else timeout
        fd = self._ensure_fd()
        deadline = time.monotonic() + limit
        while True:
            if self._try_lock(fd):
                self._held = True
                return
            if time.monotonic() >= deadline:
                raise LockTimeoutError(
                    f"could not acquire {self.path} within {limit:g}s; "
                    "another process is holding the queue lock"
                )
            time.sleep(_POLL_INTERVAL)

    def release(self) -> None:
        """Drop the lock, keeping the descriptor for the next acquire.

        Safe to call when not held.
        """
        if not self._held or self._fd is None:
            return
        self._held = False
        self._unlock(self._fd)

    def close(self) -> None:
        """Release the lock and let go of the descriptor. Idempotent."""
        self.release()
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def _ensure_fd(self) -> int:
        """Open the lock file once and keep it for the object's lifetime.

        Opening and closing it around every operation was measurably the single
        most expensive thing nobroker did -- 44% of a non-fsyncing enqueue went
        into ``open`` and ``close`` on this one file. Holding the descriptor is
        free and changes nothing about the semantics: both ``flock`` and
        ``msvcrt.locking`` associate the lock with the open file description, so
        locking and unlocking the same descriptor repeatedly is exactly as
        exclusive as reopening it each time. The kernel still drops the lock if
        the process dies, because that is the descriptor closing.
        """
        if self._fd is None:
            # O_CREAT, never O_TRUNC: the file's *contents* are irrelevant, only
            # its identity matters, and truncating would be a pointless write.
            self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        return self._fd

    @property
    def held(self) -> bool:
        return self._held

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    # -- platform shims ---------------------------------------------------

    @staticmethod
    def _try_lock(fd: int) -> bool:
        """One non-blocking attempt. True if the lock is now held."""
        if _HAVE_FCNTL:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
            return True
        if _HAVE_MSVCRT:  # pragma: no cover - Windows only
            # LK_NBLCK locks one byte from the current file position. Byte 0 is
            # as good as any: it is the identity of the lock, not data.
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                return False
            return True
        raise RuntimeError(
            "no file-locking primitive available: neither fcntl nor msvcrt "
            "could be imported, so multi-process safety cannot be guaranteed"
        )

    @staticmethod
    def _unlock(fd: int) -> None:
        if _HAVE_FCNTL:
            fcntl.flock(fd, fcntl.LOCK_UN)
        elif _HAVE_MSVCRT:  # pragma: no cover - Windows only
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                # Closing the descriptor releases the lock regardless; a failed
                # explicit unlock is not worth propagating out of a finally.
                pass

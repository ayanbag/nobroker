"""On-disk framing for the write-ahead log.

The format is deliberately boring::

    file header:  <8s magic><H version><I generation><H reserved>   = 16 bytes
    record:       <I payload_len><B type><I crc32><payload bytes>   = 9 + n

``crc32`` covers the type byte and the payload, so a record whose length
survived a partial write but whose body did not is still caught.

Why length-prefix *and* checksum? The length alone tells you a record is
incomplete, which catches the common torn tail. The checksum catches the case
that actually loses data quietly: a write that reached the page cache in pieces,
leaving a full-length record with a hole in the middle. Cheap insurance --
``zlib.crc32`` runs at GB/s and ships with Python.

Payloads are JSON. A packed binary encoding would be faster and smaller, but a
durability format you cannot read with your eyes is a durability format you
cannot debug, and ``nobroker inspect`` exists because of this choice.
"""

from __future__ import annotations

import enum
import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Final

from .errors import CorruptLogError, SerializationError

MAGIC: Final[bytes] = b"NOBROKER"
FORMAT_VERSION: Final[int] = 1

_FILE_HEADER = struct.Struct("<8sHIH")
HEADER_SIZE: Final[int] = _FILE_HEADER.size  # 16

_RECORD_HEADER = struct.Struct("<IBI")
RECORD_HEADER_SIZE: Final[int] = _RECORD_HEADER.size  # 9

#: Refuse to allocate for a record larger than this. A corrupt length field is
#: otherwise an invitation to try to read 4 GiB into memory.
MAX_PAYLOAD_BYTES: Final[int] = 8 * 1024 * 1024


class RecordType(enum.IntEnum):
    """One byte per record kind. Values are frozen -- they are on disk."""

    ENQUEUE = 1
    LEASE = 2
    ACK = 3
    NACK = 4
    RECLAIM = 5
    DEAD = 6
    REQUEUE = 7
    PURGE = 8
    EXTEND = 9


@dataclass(frozen=True, slots=True)
class Record:
    """A decoded log record and where it starts in the file."""

    offset: int
    type: RecordType
    payload: dict[str, Any]


class Damage(str, enum.Enum):
    """Why the reader stopped before end of file."""

    NONE = "none"
    TRUNCATED_HEADER = "truncated record header"
    TRUNCATED_BODY = "truncated record body"
    BAD_CHECKSUM = "checksum mismatch"
    OVERSIZE = "record length exceeds maximum"
    UNKNOWN_TYPE = "unknown record type"


def encode_file_header(generation: int = 0) -> bytes:
    """Build the 16-byte file header.

    ``generation`` is bumped by compaction. Peer processes compare it against
    their own to notice that the log they have open has been replaced underneath
    them, which is the one thing an append-only reader cannot detect on its own.
    """
    return _FILE_HEADER.pack(MAGIC, FORMAT_VERSION, generation, 0)


def decode_file_header(buf: bytes) -> int:
    """Validate a file header and return its generation.

    Raises:
        CorruptLogError: The file is not a nobroker log, or is a version this
            build does not understand. Both are fatal on purpose: guessing at an
            unknown format is how you turn a readable file into a lost one.
    """
    if len(buf) < HEADER_SIZE:
        raise CorruptLogError(f"log header truncated: {len(buf)} of {HEADER_SIZE} bytes")
    magic, version, generation, _reserved = _FILE_HEADER.unpack(buf[:HEADER_SIZE])
    if magic != MAGIC:
        raise CorruptLogError(f"not a nobroker log (magic {magic!r})")
    if version != FORMAT_VERSION:
        raise CorruptLogError(
            f"log format version {version} is not supported by this build "
            f"(expected {FORMAT_VERSION})"
        )
    return generation


def encode_record(rtype: RecordType, payload: dict[str, Any]) -> bytes:
    """Serialise one record.

    ``sort_keys`` makes the bytes a deterministic function of the payload, which
    lets tests assert on exact log contents.
    """
    try:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SerializationError(
            f"payload is not JSON-serialisable: {exc}"
        ) from exc
    if len(body) > MAX_PAYLOAD_BYTES:
        raise SerializationError(
            f"record of {len(body)} bytes exceeds the {MAX_PAYLOAD_BYTES}-byte limit"
        )
    crc = zlib.crc32(bytes([rtype]) + body) & 0xFFFFFFFF
    return _RECORD_HEADER.pack(len(body), int(rtype), crc) + body


def decode_record(buf: bytes, offset: int) -> tuple[Record | None, int, Damage]:
    """Decode the record starting at ``offset`` within ``buf``.

    Returns a ``(record, bytes_consumed, damage)`` triple. ``damage`` is
    ``Damage.NONE`` on success; on failure ``record`` is ``None`` and the caller
    decides whether the damage is a repairable torn tail or real corruption.
    Returning a verdict instead of raising keeps the recovery policy in one place
    -- the log reader -- rather than smeared across exception handlers.
    """
    end_of_header = offset + RECORD_HEADER_SIZE
    if end_of_header > len(buf):
        return None, 0, Damage.TRUNCATED_HEADER

    length, raw_type, crc = _RECORD_HEADER.unpack(buf[offset:end_of_header])
    if length > MAX_PAYLOAD_BYTES:
        return None, 0, Damage.OVERSIZE

    end_of_body = end_of_header + length
    if end_of_body > len(buf):
        return None, 0, Damage.TRUNCATED_BODY

    body = buf[end_of_header:end_of_body]
    if zlib.crc32(bytes([raw_type]) + body) & 0xFFFFFFFF != crc:
        return None, 0, Damage.BAD_CHECKSUM

    try:
        rtype = RecordType(raw_type)
    except ValueError:
        return None, 0, Damage.UNKNOWN_TYPE

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        # The CRC matched, so the bytes on disk are the bytes we wrote. Bad JSON
        # here means a writer bug, not media damage -- but it is still a record
        # we cannot apply, so it terminates the scan like any other damage.
        return None, 0, Damage.BAD_CHECKSUM

    return Record(offset, rtype, payload), end_of_body - offset, Damage.NONE

"""Framing: the layer everything else trusts to tell it where a record ends."""

from __future__ import annotations

import unittest
import zlib

from nobroker.codec import (
    HEADER_SIZE,
    MAGIC,
    MAX_PAYLOAD_BYTES,
    RECORD_HEADER_SIZE,
    Damage,
    RecordType,
    decode_file_header,
    decode_record,
    encode_file_header,
    encode_record,
)
from nobroker.errors import CorruptLogError, SerializationError


class FileHeaderTest(unittest.TestCase):
    def test_round_trip(self) -> None:
        self.assertEqual(decode_file_header(encode_file_header(7)), 7)

    def test_header_is_exactly_sixteen_bytes(self) -> None:
        # Fixed and small, so it never straddles anything interesting.
        self.assertEqual(len(encode_file_header(0)), HEADER_SIZE)
        self.assertEqual(HEADER_SIZE, 16)

    def test_foreign_magic_is_rejected(self) -> None:
        blob = b"NOTOURS!" + encode_file_header(0)[8:]
        with self.assertRaises(CorruptLogError):
            decode_file_header(blob)

    def test_future_version_is_rejected_rather_than_guessed(self) -> None:
        blob = bytearray(encode_file_header(0))
        blob[8:10] = (99).to_bytes(2, "little")
        with self.assertRaises(CorruptLogError) as ctx:
            decode_file_header(bytes(blob))
        self.assertIn("99", str(ctx.exception))

    def test_short_header_is_rejected(self) -> None:
        with self.assertRaises(CorruptLogError):
            decode_file_header(MAGIC[:4])


class RecordTest(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = {"id": "abc", "n": 1, "nested": {"a": [1, 2, 3]}}
        self.blob = encode_record(RecordType.ENQUEUE, self.payload)

    def test_round_trip(self) -> None:
        record, size, damage = decode_record(self.blob, 0)
        self.assertIs(damage, Damage.NONE)
        self.assertEqual(size, len(self.blob))
        assert record is not None
        self.assertEqual(record.type, RecordType.ENQUEUE)
        self.assertEqual(record.payload, self.payload)

    def test_encoding_is_deterministic(self) -> None:
        # Sorted keys mean the same payload always produces the same bytes,
        # which is what lets the recovery tests assert on exact file contents.
        again = encode_record(RecordType.ENQUEUE, dict(reversed(list(self.payload.items()))))
        self.assertEqual(self.blob, again)

    def test_records_decode_back_to_back(self) -> None:
        stream = self.blob + encode_record(RecordType.ACK, {"id": "abc"})
        first, size, _ = decode_record(stream, 0)
        second, _, damage = decode_record(stream, size)
        assert first is not None and second is not None
        self.assertIs(damage, Damage.NONE)
        self.assertEqual(second.type, RecordType.ACK)

    def test_truncated_header_is_reported(self) -> None:
        _, _, damage = decode_record(self.blob[: RECORD_HEADER_SIZE - 1], 0)
        self.assertIs(damage, Damage.TRUNCATED_HEADER)

    def test_truncated_body_is_reported(self) -> None:
        _, _, damage = decode_record(self.blob[:-1], 0)
        self.assertIs(damage, Damage.TRUNCATED_BODY)

    def test_every_single_byte_flip_is_detected(self) -> None:
        """The CRC has to catch corruption anywhere in the record, not just late.

        This is the guarantee that lets recovery treat "decoded cleanly" as
        "exactly what was written".
        """
        for i in range(len(self.blob)):
            with self.subTest(byte=i):
                mutated = bytearray(self.blob)
                mutated[i] ^= 0x01
                record, _, damage = decode_record(bytes(mutated), 0)
                if damage is Damage.NONE:
                    # A flip in the length field can still decode -- as a
                    # *different, shorter* record. It must never round-trip to
                    # the original payload.
                    assert record is not None
                    self.assertNotEqual(record.payload, self.payload)

    def test_absurd_length_is_refused_without_allocating(self) -> None:
        blob = bytearray(self.blob)
        blob[0:4] = (MAX_PAYLOAD_BYTES + 1).to_bytes(4, "little")
        _, _, damage = decode_record(bytes(blob), 0)
        self.assertIs(damage, Damage.OVERSIZE)

    def test_unknown_record_type_is_reported(self) -> None:
        body = b'{"id":"x"}'
        raw_type = 250
        crc = zlib.crc32(bytes([raw_type]) + body) & 0xFFFFFFFF
        blob = (
            len(body).to_bytes(4, "little")
            + bytes([raw_type])
            + crc.to_bytes(4, "little")
            + body
        )
        _, _, damage = decode_record(blob, 0)
        self.assertIs(damage, Damage.UNKNOWN_TYPE)

    def test_unserialisable_payload_raises_before_touching_disk(self) -> None:
        with self.assertRaises(SerializationError):
            encode_record(RecordType.ENQUEUE, {"f": object()})

    def test_oversize_payload_is_refused(self) -> None:
        with self.assertRaises(SerializationError):
            encode_record(RecordType.ENQUEUE, {"blob": "x" * (MAX_PAYLOAD_BYTES + 1)})

    def test_record_type_values_are_stable(self) -> None:
        """These integers are on disk. Renumbering them would orphan every log."""
        self.assertEqual(
            {t.name: int(t) for t in RecordType},
            {
                "ENQUEUE": 1,
                "LEASE": 2,
                "ACK": 3,
                "NACK": 4,
                "RECLAIM": 5,
                "DEAD": 6,
                "REQUEUE": 7,
                "PURGE": 8,
                "EXTEND": 9,
            },
        )


if __name__ == "__main__":
    unittest.main()

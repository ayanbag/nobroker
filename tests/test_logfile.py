"""The append-only log and the generation layout, tested directly."""

from __future__ import annotations


import unittest

from nobroker.codec import HEADER_SIZE, Damage, RecordType
from nobroker.logfile import Layout, LogFile

from .support import TempQueueTest


class LogFileTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        self.log = LogFile(self.dir / "test.log")
        self.log.open()
        self.addCleanup(self.log.close)

    def test_a_new_log_is_just_a_header(self) -> None:
        self.assertEqual(self.log.end_offset, HEADER_SIZE)
        records, end, damage = self.log.scan(HEADER_SIZE)
        self.assertEqual(records, [])
        self.assertEqual(end, HEADER_SIZE)
        self.assertIs(damage, Damage.NONE)

    def test_append_and_scan_round_trip(self) -> None:
        payloads = [{"id": str(i), "n": i} for i in range(5)]
        self.log.append([(RecordType.ENQUEUE, p) for p in payloads])
        records, _, damage = self.log.scan(HEADER_SIZE)
        self.assertIs(damage, Damage.NONE)
        self.assertEqual([r.payload for r in records], payloads)

    def test_offsets_are_where_the_records_are(self) -> None:
        self.log.append([(RecordType.ENQUEUE, {"id": "a"})])
        records, _, _ = self.log.scan(HEADER_SIZE)
        self.assertEqual(records[0].offset, HEADER_SIZE)

    def test_incremental_scan_only_returns_new_records(self) -> None:
        self.log.append([(RecordType.ENQUEUE, {"id": "a"})])
        _, first_end, _ = self.log.scan(HEADER_SIZE)
        self.log.append([(RecordType.ACK, {"id": "a"})])

        records, _, _ = self.log.scan(first_end)
        self.assertEqual([r.type for r in records], [RecordType.ACK])

    def test_records_larger_than_one_read_chunk(self) -> None:
        """A record can span chunk boundaries; the carry buffer has to cope."""
        big = {"id": "big", "blob": "x" * (3 * 1024 * 1024)}
        self.log.append([(RecordType.ENQUEUE, big), (RecordType.ACK, {"id": "big"})])
        records, _, damage = self.log.scan(HEADER_SIZE)
        self.assertIs(damage, Damage.NONE)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].payload, big)

    def test_truncate_reports_what_it_removed(self) -> None:
        self.log.append([(RecordType.ENQUEUE, {"id": "a"})])
        end = self.log.end_offset
        self.log.append([(RecordType.ENQUEUE, {"id": "b"})])
        discarded = self.log.truncate(end)
        self.assertGreater(discarded, 0)
        self.assertEqual(self.log.end_offset, end)
        records, _, _ = self.log.scan(HEADER_SIZE)
        self.assertEqual(len(records), 1)

    def test_appending_nothing_writes_nothing(self) -> None:
        before = self.log.end_offset
        self.assertEqual(self.log.append([]), before)

    def test_written_bytes_are_the_bytes_we_asked_for(self) -> None:
        """No newline translation anywhere in the write path.

        Regression test. On Windows a descriptor opened without ``O_BINARY`` is
        in text mode and the C runtime rewrites every ``0x0A`` as ``0x0D 0x0A``.
        For a checksummed binary log that is corruption, and it surfaces only
        when a record happens to contain a newline byte -- so it presented as an
        intermittent CRC failure in a completely different part of the system.
        """
        from nobroker.codec import encode_file_header, encode_record

        # JSON escapes newlines, so a 0x0A never reaches disk from the payload.
        # The exposed bytes are the *binary* header fields. `{"a":"bc"}` encodes
        # to exactly 10 bytes, which makes the little-endian length prefix
        # b"\x0a\x00\x00\x00" -- a literal newline in the frame itself. Plus a
        # spread of records so the pseudo-random CRC bytes get a turn too.
        payloads = [{"a": "bc"}] + [{"id": f"job-{i}", "n": i} for i in range(200)]
        records = [(RecordType.ENQUEUE, p) for p in payloads]

        expected = encode_file_header(0) + b"".join(
            encode_record(rtype, p) for rtype, p in records
        )
        self.assertIn(b"\n", expected, "the fixture must contain a newline byte")

        self.log.append(records)
        self.log.close()
        self.assertEqual(self.log.path.read_bytes(), expected)

    def test_a_stale_end_offset_is_refreshed_by_scanning(self) -> None:
        """The multi-process bug, pinned down at the layer where it lived.

        Two handles on one file: if the second appends using its own cached end
        offset, it lands on top of the first one's record.
        """
        peer = LogFile(self.log.path)
        peer.open()
        self.addCleanup(peer.close)
        self.assertEqual(peer.end_offset, HEADER_SIZE)

        self.log.append([(RecordType.ENQUEUE, {"id": "written-by-owner"})])

        peer.scan(HEADER_SIZE)  # what catch-up does before every operation
        peer.append([(RecordType.ENQUEUE, {"id": "written-by-peer"})])

        records, _, damage = self.log.scan(HEADER_SIZE)
        self.assertIs(damage, Damage.NONE)
        self.assertEqual(
            [r.payload["id"] for r in records],
            ["written-by-owner", "written-by-peer"],
        )


class LayoutTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        self.layout = Layout(self.dir, "jobs")

    def test_a_new_queue_is_generation_zero(self) -> None:
        self.assertEqual(self.layout.read_generation(), 0)
        self.assertTrue(str(self.layout.log(0)).endswith("jobs.000000.log"))

    def test_generation_round_trip(self) -> None:
        self.layout.write_generation(7)
        self.assertEqual(self.layout.read_generation(), 7)

    def test_a_corrupt_pointer_falls_back_rather_than_failing(self) -> None:
        self.layout.pointer.write_text("not a number", encoding="ascii")
        self.assertEqual(self.layout.read_generation(), 0)

    def test_stale_logs_are_identified_oldest_first(self) -> None:
        for generation in range(4):
            self.layout.log(generation).write_bytes(b"")
        stale = self.layout.stale_logs(keep=3)
        self.assertEqual([p.name for p in stale], [
            "jobs.000000.log",
            "jobs.000001.log",
            "jobs.000002.log",
        ])

    def test_unrelated_files_are_ignored(self) -> None:
        (self.dir / "jobs.notanumber.log").write_bytes(b"")
        (self.dir / "other.000001.log").write_bytes(b"")
        self.assertEqual(self.layout.stale_logs(keep=99), [])


if __name__ == "__main__":
    unittest.main()

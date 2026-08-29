"""Crash recovery: the tests the whole project stands on.

Three levels of severity, weakest to strongest:

1. **Reopen.** State survives a clean close and reopen. Table stakes.
2. **Hard kill.** A process is destroyed mid-flight with no unwinding, no atexit,
   no buffer flush -- ``os._exit(9)``, the in-process equivalent of ``kill -9``
   and deliberately not a ``kill`` subprocess, which would be a hidden
   dependency on an external tool.
3. **Torn tail at every byte.** The exhaustive one. Take a real log, truncate it
   at *every single offset*, and assert that recovery lands on a consistent state
   at each: every record that was complete is honoured, every record that was not
   is discarded, and nothing in between is ever observed.

Level 3 is the one that actually proves the claim. A crash can only interrupt a
write at a byte boundary, so if recovery is correct at all N boundaries of a real
log, it is correct for any crash that log could have suffered.
"""

from __future__ import annotations

import os
import unittest
from pathlib import Path

from nobroker import BackoffPolicy, Job, Queue
from nobroker.codec import HEADER_SIZE

from .support import TempQueueTest, run_child_and_kill

# -- child entry points -------------------------------------------------
# Module level so they survive pickling on platforms without fork.


def child_enqueue_then_die(path: str, count: int) -> None:
    """Durably enqueue ``count`` jobs, then vanish without cleanup."""
    queue = Queue(path)
    for i in range(count):
        queue.enqueue({"n": i})
    os._exit(9)


def child_lease_then_die(path: str) -> None:
    """Lease every available job and die still holding the leases."""
    queue = Queue(path, visibility_timeout=0.2)
    queue.lease(100)
    os._exit(9)


def child_ack_half_then_die(path: str) -> None:
    """Ack half of what it leased, then die holding the rest."""
    queue = Queue(path, visibility_timeout=0.2)
    jobs = queue.lease(10)
    for job in jobs[: len(jobs) // 2]:
        queue.ack(job)
    os._exit(9)


class HardKillTest(TempQueueTest):
    """Level 2: a process destroyed with no chance to clean up."""

    def test_enqueued_jobs_survive_a_hard_kill(self) -> None:
        status = run_child_and_kill(child_enqueue_then_die, str(self.dir), 50)
        self.assertNotEqual(status, 0, "child was supposed to die abnormally")

        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        stats = queue.stats()
        # enqueue() fsyncs before returning, so every job it accepted is on disk.
        self.assertEqual(stats.total, 50)
        self.assertEqual(stats.ready, 50)
        self.assertFalse(
            queue.recovery.repaired,
            "a clean sequence of completed enqueues should need no repair",
        )

    def test_leases_held_by_a_dead_process_are_reclaimed(self) -> None:
        queue = Queue(self.dir, visibility_timeout=0.2)
        queue.enqueue_many([{"n": i} for i in range(5)])
        queue.close()

        run_child_and_kill(child_lease_then_die, str(self.dir))

        queue = Queue(self.dir, visibility_timeout=0.2)
        self.addCleanup(queue.close)
        # The dead process's leases are still recorded, so the jobs are invisible.
        self.assertEqual(queue.stats().leased, 5)

        # Once the visibility timeout passes they come back on their own. Nobody
        # had to notice the process died: the deadline did the work.
        self._advance_past_visibility_timeout(queue)
        self.assertEqual(queue.stats().ready + queue.stats().delayed, 5)

    def test_acked_work_is_not_redelivered_after_a_crash(self) -> None:
        queue = Queue(self.dir, visibility_timeout=0.2)
        queue.enqueue_many([{"n": i} for i in range(10)])
        queue.close()

        run_child_and_kill(child_ack_half_then_die, str(self.dir))

        # No backoff, so a reclaimed job is instantly available again and the
        # test does not have to sleep through a retry delay.
        queue = Queue(
            self.dir,
            visibility_timeout=0.2,
            backoff=BackoffPolicy(base=0.0, jitter=0.0),
        )
        self.addCleanup(queue.close)
        self.assertEqual(queue.stats().done, 5, "acks reached disk before the kill")
        self.assertEqual(queue.stats().leased, 5, "the rest are still held")

        self._advance_past_visibility_timeout(queue)
        recovered = queue.lease(10)
        self.assertEqual(len(recovered), 5, "exactly the unacked jobs come back")

    def _advance_past_visibility_timeout(self, queue: Queue) -> None:
        """Wait out a short visibility timeout and force a reap."""
        import time

        time.sleep(0.25)
        queue.stats()  # any operation reaps expired leases


class TornTailTest(TempQueueTest):
    """Level 3: truncate a real log at every byte offset and recover from each."""

    def _build_log(self, jobs: int = 8) -> tuple[bytes, list[str]]:
        """Produce a realistic log -- enqueues, leases and acks -- and its ids."""
        queue = Queue(self.dir)
        ids = [queue.enqueue({"n": i}).id for i in range(jobs)]
        for job in queue.lease(3):
            queue.ack(job)
        queue.close()
        return self._log_path().read_bytes(), ids

    def _log_path(self) -> Path:
        matches = sorted(self.dir.glob("default.*.log"))
        self.assertTrue(matches, "expected a log file")
        return matches[-1]

    @unittest.skipIf(
        os.environ.get("NOBROKER_SKIP_SLOW"),
        "exhaustive truncation sweep; run `make test` for the real thing",
    )
    def test_truncation_at_every_offset_recovers_consistently(self) -> None:
        blob, ids = self._build_log()
        self.assertGreater(len(blob), HEADER_SIZE + 100)

        path = self._log_path()
        previous_total = 0
        for cut in range(HEADER_SIZE, len(blob) + 1):
            with self.subTest(cut=cut):
                path.write_bytes(blob[:cut])
                queue = Queue(self.dir)
                try:
                    stats = queue.stats()
                    # Recovery is monotonic: keeping more bytes can only ever
                    # reveal more history, never less. A dip here would mean a
                    # record was decoded at one length and lost at a longer one.
                    self.assertGreaterEqual(stats.total, previous_total)
                    previous_total = stats.total
                    self.assertLessEqual(stats.total, len(ids))
                    # Whatever survived must be internally consistent.
                    for job in queue.list_jobs():
                        self.assertIsInstance(job, Job)
                        self.assertIn(job.id, ids)
                finally:
                    queue.close()

        # The untruncated log must recover everything.
        path.write_bytes(blob)
        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        self.assertEqual(queue.stats().total, len(ids))

    def test_a_torn_tail_is_truncated_and_reported(self) -> None:
        blob, _ = self._build_log(jobs=4)
        path = self._log_path()
        # Cut three bytes off the last record: a classic interrupted write.
        path.write_bytes(blob[:-3])

        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        report = queue.recovery
        self.assertTrue(report.repaired, "the damage should be reported, not hidden")
        # The *whole* incomplete record goes, not just the three missing bytes --
        # a record is only meaningful in one piece.
        self.assertGreaterEqual(report.bytes_discarded, 3)
        # The repair is durable: the garbage is gone from the file, not merely
        # ignored in memory. Reopening must not find it again.
        self.assertEqual(path.stat().st_size, len(blob) - 3 - report.bytes_discarded)
        self.assertFalse(Queue(self.dir).recovery.repaired)

    def test_a_flipped_bit_is_caught_by_the_checksum(self) -> None:
        blob, _ = self._build_log(jobs=6)
        path = self._log_path()
        # Corrupt one byte inside the payload of the *first* record. The length
        # prefix still says the record is complete and the scan still finds the
        # right number of bytes; only the CRC knows anything is wrong. Because
        # the damage is at the front, every later record is unreachable too.
        corrupted = bytearray(blob)
        corrupted[HEADER_SIZE + 20] ^= 0xFF
        path.write_bytes(bytes(corrupted))

        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        self.assertTrue(
            queue.recovery.repaired,
            "a payload bit flip must be detected, not silently applied",
        )
        self.assertEqual(
            queue.stats().total,
            0,
            "nothing after unresolvable damage may be trusted",
        )

    def test_recovery_is_deterministic(self) -> None:
        """Replaying the same log twice must produce identical state.

        This is the property that makes everything else testable: because no
        handler reads the clock or calls random, the index is a pure function of
        the bytes on disk.
        """
        blob, _ = self._build_log(jobs=8)
        self._log_path().write_bytes(blob)

        snapshots = []
        for _ in range(2):
            queue = Queue(self.dir)
            snapshots.append([job.to_dict() for job in queue.list_jobs()])
            queue.close()
        self.assertEqual(snapshots[0], snapshots[1])


class EmptyAndDegenerateLogTest(TempQueueTest):
    """Recovery has to cope with logs that barely exist."""

    def test_missing_log_is_created(self) -> None:
        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        self.assertEqual(queue.stats().total, 0)
        self.assertTrue(queue.path.exists())

    def test_header_only_log_is_clean(self) -> None:
        Queue(self.dir).close()
        path = sorted(self.dir.glob("default.*.log"))[-1]
        self.assertEqual(path.stat().st_size, HEADER_SIZE)

        queue = Queue(self.dir)
        self.addCleanup(queue.close)
        self.assertFalse(queue.recovery.repaired)

    def test_zero_length_file_is_rejected_loudly(self) -> None:
        """A truncated *header* is not a torn tail and must not be guessed at."""
        from nobroker.errors import CorruptLogError

        Queue(self.dir).close()
        path = sorted(self.dir.glob("default.*.log"))[-1]
        path.write_bytes(b"")
        with self.assertRaises(CorruptLogError):
            Queue(self.dir)

    def test_foreign_file_is_rejected(self) -> None:
        from nobroker.errors import CorruptLogError

        Queue(self.dir).close()
        path = sorted(self.dir.glob("default.*.log"))[-1]
        path.write_bytes(b"this is somebody else's file entirely")
        with self.assertRaises(CorruptLogError):
            Queue(self.dir)


if __name__ == "__main__":
    unittest.main()

"""Queue semantics: leasing, priorities, delays, retries, the DLQ, compaction."""

from __future__ import annotations

import time
import unittest

from nobroker import (
    BackoffPolicy,
    JobNotFoundError,
    JobState,
    NotLeasedError,
    Queue,
)

from .support import TempQueueTest

#: Deterministic retries: no jitter, no waiting, so tests assert on state rather
#: than on the clock.
IMMEDIATE = BackoffPolicy(base=0.0, jitter=0.0)


class BasicsTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        self.q = Queue(self.dir, backoff=IMMEDIATE)
        self.addCleanup(self.q.close)

    def test_enqueue_lease_ack(self) -> None:
        job = self.q.enqueue({"task": "hello"})
        self.assertIs(job.state, JobState.READY)

        leased = self.q.lease_one()
        assert leased is not None
        self.assertEqual(leased.id, job.id)
        self.assertIs(leased.state, JobState.LEASED)
        self.assertEqual(leased.attempts, 1, "leasing is what counts as an attempt")

        self.q.ack(leased)
        self.assertEqual(self.q.stats().done, 1)

    def test_a_leased_job_is_invisible_to_everyone_else(self) -> None:
        self.q.enqueue("only one")
        self.assertIsNotNone(self.q.lease_one())
        self.assertIsNone(self.q.lease_one(), "a leased job must not be handed out twice")

    def test_lease_returns_what_it_can(self) -> None:
        self.q.enqueue_many(["a", "b"])
        self.assertEqual(len(self.q.lease(10)), 2)

    def test_lease_of_an_empty_queue_returns_empty(self) -> None:
        self.assertEqual(self.q.lease(5), [])
        self.assertIsNone(self.q.lease_one())

    def test_payloads_survive_a_round_trip_unchanged(self) -> None:
        payloads = [None, 0, "", "text", 3.5, True, [1, {"a": None}], {"k": [1, 2]}]
        self.q.enqueue_many(payloads)
        # enqueue_many applies one set of options to all jobs, so ordering is FIFO.
        got = [job.payload for job in self.q.list_jobs()]
        self.assertEqual(got, payloads)

    def test_acking_twice_is_not_an_error(self) -> None:
        job = self.q.enqueue("x")
        leased = self.q.lease_one()
        assert leased is not None
        self.q.ack(leased)
        self.q.ack(leased)  # a retried ack after a network blip must be harmless
        self.assertEqual(self.q.stats().done, 1)
        self.assertEqual(self.q.get(job.id).state, JobState.DONE)

    def test_acking_an_unleased_job_is_refused(self) -> None:
        job = self.q.enqueue("x")
        with self.assertRaises(NotLeasedError):
            self.q.ack(job.id)

    def test_unknown_job_id_is_refused(self) -> None:
        with self.assertRaises(JobNotFoundError):
            self.q.get("nope")

    def test_len_counts_every_job(self) -> None:
        self.q.enqueue_many(["a", "b", "c"])
        self.assertEqual(len(self.q), 3)


class OrderingTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        self.q = Queue(self.dir, backoff=IMMEDIATE)
        self.addCleanup(self.q.close)

    def test_higher_priority_leases_first(self) -> None:
        self.q.enqueue("low", priority=0)
        self.q.enqueue("high", priority=10)
        self.q.enqueue("middle", priority=5)
        order = [job.payload for job in self.q.lease(3)]
        self.assertEqual(order, ["high", "middle", "low"])

    def test_equal_priority_is_fifo(self) -> None:
        self.q.enqueue_many([f"job-{i}" for i in range(20)])
        order = [job.payload for job in self.q.lease(20)]
        self.assertEqual(order, [f"job-{i}" for i in range(20)])

    def test_a_delayed_high_priority_job_does_not_block_the_queue(self) -> None:
        """The bug two heaps exist to prevent.

        With one heap, the urgent-but-not-yet-due job sits at the head and
        everything behind it starves until its delay elapses.
        """
        self.q.enqueue("urgent later", priority=100, delay=30.0)
        self.q.enqueue("ordinary now", priority=0)
        leased = self.q.lease(2)
        self.assertEqual([job.payload for job in leased], ["ordinary now"])

    def test_delayed_jobs_become_available_on_time(self) -> None:
        self.q.enqueue("soon", delay=0.15)
        self.assertIsNone(self.q.lease_one())
        self.assertEqual(self.q.stats().delayed, 1)
        time.sleep(0.2)
        leased = self.q.lease_one()
        assert leased is not None
        self.assertEqual(leased.payload, "soon")


class RetryTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        self.q = Queue(self.dir, backoff=IMMEDIATE, max_attempts=3)
        self.addCleanup(self.q.close)

    def test_nack_returns_the_job_for_another_attempt(self) -> None:
        self.q.enqueue("flaky")
        first = self.q.lease_one()
        assert first is not None
        updated = self.q.nack(first, error="boom")
        self.assertIs(updated.state, JobState.READY)
        self.assertEqual(updated.last_error, "boom")

        second = self.q.lease_one()
        assert second is not None
        self.assertEqual(second.id, first.id)
        self.assertEqual(second.attempts, 2)

    def test_exhausted_attempts_land_in_the_dlq(self) -> None:
        self.q.enqueue("doomed")
        for expected in range(1, 4):
            job = self.q.lease_one()
            assert job is not None, f"expected delivery {expected}"
            self.assertEqual(job.attempts, expected)
            self.q.nack(job, error="always fails")

        self.assertIsNone(self.q.lease_one(), "a dead job is not leasable")
        dead = self.q.dlq()
        self.assertEqual(len(dead), 1)
        self.assertIs(dead[0].state, JobState.DEAD)

    def test_backoff_delays_the_retry(self) -> None:
        q = Queue(self.dir, "slow", backoff=BackoffPolicy(base=30.0, jitter=0.0))
        self.addCleanup(q.close)
        q.enqueue("later")
        job = q.lease_one()
        assert job is not None
        q.nack(job)
        self.assertIsNone(q.lease_one(), "the retry must wait out the backoff")
        self.assertEqual(q.stats().delayed, 1)

    def test_explicit_delay_overrides_the_policy(self) -> None:
        self.q.enqueue("x")
        job = self.q.lease_one()
        assert job is not None
        updated = self.q.nack(job, delay=60.0)
        self.assertGreater(updated.available_at, time.time() + 30)

    def test_per_job_max_attempts_is_honoured(self) -> None:
        self.q.enqueue("fragile", max_attempts=1)
        job = self.q.lease_one()
        assert job is not None
        updated = self.q.nack(job)
        self.assertIs(updated.state, JobState.DEAD, "one attempt, one chance")

    def test_requeue_revives_the_dlq(self) -> None:
        self.q.enqueue("doomed")
        for _ in range(3):
            job = self.q.lease_one()
            assert job is not None
            self.q.nack(job)
        self.assertEqual(self.q.requeue_dead(), 1)

        revived = self.q.lease_one()
        assert revived is not None
        self.assertEqual(revived.attempts, 1, "the attempt counter resets")


class VisibilityTimeoutTest(TempQueueTest):
    def test_an_expired_lease_returns_the_job(self) -> None:
        q = Queue(self.dir, visibility_timeout=0.1, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("slow handler")
        first = q.lease_one()
        assert first is not None

        time.sleep(0.15)
        second = q.lease_one()
        assert second is not None
        self.assertEqual(second.id, first.id)
        self.assertEqual(second.attempts, 2)

    def test_the_original_holder_cannot_ack_after_losing_the_lease(self) -> None:
        """Fencing. The late worker must be told, not silently obeyed."""
        q = Queue(self.dir, visibility_timeout=0.1, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("contested")
        first = q.lease_one()
        assert first is not None

        time.sleep(0.15)
        second = q.lease_one()  # redelivered to somebody else
        assert second is not None
        self.assertNotEqual(first.lease_token, second.lease_token)

        with self.assertRaises(NotLeasedError):
            q.ack(first)
        q.ack(second)  # the current holder is fine

    def test_extend_keeps_a_slow_job_from_being_redelivered(self) -> None:
        q = Queue(self.dir, visibility_timeout=0.2, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("long running")
        job = q.lease_one()
        assert job is not None

        time.sleep(0.15)
        q.extend(job, 5.0)
        time.sleep(0.15)
        self.assertIsNone(q.lease_one(), "the extended lease still holds")
        q.ack(job)

    def test_an_expired_lease_can_exhaust_attempts(self) -> None:
        q = Queue(self.dir, visibility_timeout=0.05, max_attempts=2, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("nobody ever acks this")
        for _ in range(2):
            self.assertIsNotNone(q.lease_one())
            time.sleep(0.08)
        self.assertEqual(q.stats().dead, 1, "silent workers still exhaust attempts")


class IdempotentEnqueueTest(TempQueueTest):
    def test_a_repeated_job_id_does_not_double_schedule(self) -> None:
        q = Queue(self.dir)
        self.addCleanup(q.close)
        first = q.enqueue({"charge": 100}, job_id="order-42")
        second = q.enqueue({"charge": 100}, job_id="order-42")
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(q), 1, "a retried request must not enqueue twice")

    def test_dedupe_survives_a_restart(self) -> None:
        q = Queue(self.dir)
        q.enqueue("once", job_id="k")
        q.close()
        q = Queue(self.dir)
        self.addCleanup(q.close)
        q.enqueue("once", job_id="k")
        self.assertEqual(len(q), 1)


class MaintenanceTest(TempQueueTest):
    def test_purge_removes_everything(self) -> None:
        q = Queue(self.dir)
        self.addCleanup(q.close)
        q.enqueue_many(["a", "b", "c"])
        self.assertEqual(q.purge(), 3)
        self.assertEqual(len(q), 0)
        self.assertIsNone(q.lease_one())

    def test_purge_is_durable(self) -> None:
        q = Queue(self.dir)
        q.enqueue_many(["a", "b"])
        q.purge()
        q.close()
        q = Queue(self.dir)
        self.addCleanup(q.close)
        self.assertEqual(len(q), 0)

    def test_compaction_drops_completed_jobs_and_keeps_the_rest(self) -> None:
        q = Queue(self.dir, backoff=IMMEDIATE, max_attempts=1)
        self.addCleanup(q.close)
        q.enqueue_many([f"done-{i}" for i in range(20)])
        for job in q.lease(20):
            q.ack(job)
        q.enqueue("still waiting")
        q.enqueue("will die")
        doomed = [j for j in q.lease(2) if j.payload == "will die"][0]
        q.nack(doomed)

        result = q.compact()
        self.assertEqual(result.jobs_dropped, 20)
        self.assertGreater(result.bytes_reclaimed, 0)

        stats = q.stats()
        self.assertEqual(stats.done, 0, "completed history is what compaction removes")
        self.assertEqual(stats.dead, 1, "the DLQ is not maintenance debris")
        self.assertEqual(stats.total, 2)

    def test_a_compacted_queue_reopens_identically(self) -> None:
        q = Queue(self.dir, backoff=IMMEDIATE)
        q.enqueue_many([f"job-{i}" for i in range(10)])
        for job in q.lease(4):
            q.ack(job)
        q.enqueue("delayed", delay=60.0)
        q.compact()
        before = [j.to_dict() for j in q.list_jobs()]
        q.close()

        q = Queue(self.dir)
        self.addCleanup(q.close)
        self.assertEqual([j.to_dict() for j in q.list_jobs()], before)

    def test_leases_survive_compaction(self) -> None:
        """A leased job stored as a snapshot must still time out afterwards."""
        q = Queue(self.dir, visibility_timeout=0.1, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("held")
        q.lease_one()
        q.compact()
        self.assertEqual(q.stats().leased, 1)
        time.sleep(0.15)
        self.assertIsNotNone(q.lease_one(), "the visibility timeout still fires")

    def test_compaction_preserves_payloads_containing_newlines(self) -> None:
        """Regression: compaction wrote through a text-mode descriptor.

        On Windows that turned every ``0x0A`` in the compacted log into
        ``0x0D 0x0A``, which broke the checksum of any record unlucky enough to
        contain one -- so the queue lost jobs at random on the next open.
        """
        q = Queue(self.dir)
        self.addCleanup(q.close)
        payloads = [{"body": "line one\nline two\n" * 8, "n": i} for i in range(20)]
        q.enqueue_many(payloads)
        q.compact()
        q.close()

        reopened = Queue(self.dir)
        self.addCleanup(reopened.close)
        self.assertFalse(reopened.recovery.repaired, "the compacted log is intact")
        self.assertEqual([job.payload for job in reopened.list_jobs()], payloads)

    def test_compacting_an_empty_queue_is_harmless(self) -> None:
        q = Queue(self.dir)
        self.addCleanup(q.close)
        result = q.compact()
        self.assertEqual(result.jobs_kept, 0)
        self.assertEqual(len(q), 0)


class NamedQueueTest(TempQueueTest):
    def test_queues_in_one_directory_are_independent(self) -> None:
        emails = Queue(self.dir, "emails")
        thumbs = Queue(self.dir, "thumbnails")
        self.addCleanup(emails.close)
        self.addCleanup(thumbs.close)

        emails.enqueue("send")
        self.assertEqual(len(emails), 1)
        self.assertEqual(len(thumbs), 0, "queues must not see each other's work")

        thumbs.enqueue("resize")
        job = emails.lease_one()
        assert job is not None
        self.assertEqual(job.payload, "send")


class ClosedQueueTest(TempQueueTest):
    def test_using_a_closed_queue_is_refused(self) -> None:
        from nobroker import QueueClosedError

        q = Queue(self.dir)
        q.close()
        with self.assertRaises(QueueClosedError):
            q.enqueue("x")

    def test_close_is_idempotent(self) -> None:
        q = Queue(self.dir)
        q.close()
        q.close()

    def test_context_manager_closes(self) -> None:
        from nobroker import QueueClosedError

        with Queue(self.dir) as q:
            q.enqueue("x")
        with self.assertRaises(QueueClosedError):
            q.stats()


if __name__ == "__main__":
    unittest.main()

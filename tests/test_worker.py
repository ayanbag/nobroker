"""The worker runner: outcomes, shutdown, and the heartbeat."""

from __future__ import annotations

import logging
import threading
import time
import unittest

from nobroker import BackoffPolicy, Job, Queue, Worker

from .support import TempQueueTest

IMMEDIATE = BackoffPolicy(base=0.0, jitter=0.0)


class WorkerTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        # Several tests deliberately fail jobs; the worker logs each one, which
        # would bury the actual test output.
        logger = logging.getLogger("nobroker.worker")
        previous = logger.level
        logger.setLevel(logging.CRITICAL)
        self.addCleanup(logger.setLevel, previous)

        self.q = Queue(self.dir, backoff=IMMEDIATE, max_attempts=2)
        self.addCleanup(self.q.close)

    def test_successful_jobs_are_acked(self) -> None:
        self.q.enqueue_many([{"n": i} for i in range(5)])
        seen: list[int] = []

        stats = Worker(
            self.q, lambda job: seen.append(job.payload["n"]), idle_timeout=0.0
        ).run()

        self.assertEqual(sorted(seen), list(range(5)))
        self.assertEqual(stats.succeeded, 5)
        self.assertEqual(self.q.stats().done, 5)

    def test_a_raising_handler_nacks_and_retries(self) -> None:
        self.q.enqueue("flaky")
        calls: list[int] = []

        def handler(job: Job) -> None:
            calls.append(job.attempts)
            if job.attempts < 2:
                raise RuntimeError("not yet")

        stats = Worker(self.q, handler, idle_timeout=0.0).run()

        self.assertEqual(calls, [1, 2], "the job came back for a second attempt")
        self.assertEqual(stats.failed, 1)
        self.assertEqual(stats.succeeded, 1)
        self.assertEqual(self.q.stats().done, 1)

    def test_a_permanently_failing_job_reaches_the_dlq(self) -> None:
        self.q.enqueue("doomed")

        def handler(job: Job) -> None:
            raise ValueError("always broken")

        stats = Worker(self.q, handler, idle_timeout=0.0).run()

        self.assertEqual(stats.dead, 1)
        dead = self.q.dlq()
        self.assertEqual(len(dead), 1)
        self.assertIn("always broken", dead[0].last_error or "")
        self.assertIn("ValueError", dead[0].last_error or "")

    def test_max_jobs_stops_the_worker(self) -> None:
        self.q.enqueue_many([{"n": i} for i in range(10)])
        stats = Worker(self.q, lambda job: None, max_jobs=3).run()
        self.assertEqual(stats.leased, 3)
        self.assertEqual(self.q.stats().ready, 7)

    def test_idle_timeout_stops_the_worker(self) -> None:
        start = time.monotonic()
        stats = Worker(self.q, lambda job: None, idle_timeout=0.1, poll_interval=0.01).run()
        self.assertLess(time.monotonic() - start, 5.0)
        self.assertEqual(stats.leased, 0)

    def test_stop_lets_the_current_job_finish(self) -> None:
        """Shutdown must never abandon work that is already running."""
        self.q.enqueue("in flight")
        entered = threading.Event()
        finished = threading.Event()

        def handler(job: Job) -> None:
            entered.set()
            time.sleep(0.2)
            finished.set()

        worker = Worker(self.q, handler, poll_interval=0.01, handle_signals=False)
        thread = threading.Thread(target=worker.run)
        thread.start()

        entered.wait(timeout=5)
        worker.stop()
        thread.join(timeout=10)

        self.assertTrue(finished.is_set(), "the in-flight handler was cut short")
        self.assertEqual(self.q.stats().done, 1, "and its result was still acked")

    def test_concurrent_handlers_share_the_queue_safely(self) -> None:
        total = 60
        self.q.enqueue_many([{"n": i} for i in range(total)])
        seen: list[int] = []
        lock = threading.Lock()

        def handler(job: Job) -> None:
            with lock:
                seen.append(job.payload["n"])

        stats = Worker(self.q, handler, concurrency=6, idle_timeout=0.0).run()

        self.assertEqual(stats.succeeded, total)
        self.assertEqual(len(set(seen)), total, "a job ran twice")

    def test_a_purge_mid_flight_does_not_kill_the_worker(self) -> None:
        """A job that stops existing while its handler runs must not end a thread.

        Purging is the only way a leased job disappears, and an operator doing it
        while work is in flight is ordinary. Before this was handled, ``nack``
        raised ``JobNotFoundError`` out of ``_run_one``, which ended the worker
        thread -- so a pool would shrink to nothing and go quiet without ever
        reporting that it had stopped consuming.
        """
        self.q.enqueue("in flight")
        entered = threading.Event()
        release = threading.Event()
        seen: list[str] = []

        def handler(job: Job) -> None:
            seen.append(job.id)
            if entered.is_set():
                return  # the job enqueued after the purge: succeed normally
            entered.set()
            release.wait(timeout=5)
            raise RuntimeError("failed after its job was purged")

        worker = Worker(self.q, handler, poll_interval=0.01, handle_signals=False)
        thread = threading.Thread(target=worker.run)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            self.q.purge()
            release.set()

            # The real assertion: the worker is still able to take new work.
            self.q.enqueue("after the purge")
            deadline = time.monotonic() + 5
            while len(seen) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            worker.stop()
            thread.join(timeout=10)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(seen), 2, "the worker thread died when the job vanished")
        self.assertEqual(self.q.stats().done, 1, "the later job still acked")

    def test_a_purge_mid_flight_is_survivable_on_the_success_path(self) -> None:
        """The same hole existed in ``ack``: no job to ack is not an error."""
        self.q.enqueue("in flight")
        entered = threading.Event()
        release = threading.Event()

        def handler(job: Job) -> None:
            entered.set()
            release.wait(timeout=5)  # returning means ack, and the job is gone

        worker = Worker(self.q, handler, poll_interval=0.01, handle_signals=False)
        thread = threading.Thread(target=worker.run)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            self.q.purge()
            release.set()
        finally:
            worker.stop()
            thread.join(timeout=10)

        self.assertFalse(thread.is_alive())
        self.assertEqual(self.q.stats().total, 0)

    def test_the_heartbeat_protects_a_slow_handler(self) -> None:
        """A handler slower than the visibility timeout must not be redelivered.

        Without the heartbeat this is the classic duplicate-work bug: the worker
        is perfectly healthy, just slow, and the queue hands its job to somebody
        else halfway through.
        """
        q = Queue(self.dir, "slow", visibility_timeout=0.15, backoff=IMMEDIATE)
        self.addCleanup(q.close)
        q.enqueue("takes a while")
        runs: list[float] = []

        def handler(job: Job) -> None:
            runs.append(time.monotonic())
            time.sleep(0.5)  # more than three visibility timeouts

        Worker(q, handler, idle_timeout=0.0, poll_interval=0.02).run()

        self.assertEqual(len(runs), 1, "the slow job was redelivered mid-flight")
        self.assertEqual(q.stats().done, 1)


if __name__ == "__main__":
    unittest.main()

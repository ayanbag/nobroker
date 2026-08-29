"""Concurrency: many threads, and many *processes*, against one queue.

The multi-process tests are the ones that justify the project's existence. If a
job can be leased twice at the same moment by two processes, nobroker is a toy
and you should go and run Redis. So they are written to fail loudly if the file
lock is wrong: every worker writes down exactly which job ids it was handed, and
the parent asserts the sets are disjoint and complete.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import threading
import unittest
from pathlib import Path

from nobroker import BackoffPolicy, Queue

from .support import TempQueueTest

IMMEDIATE = BackoffPolicy(base=0.0, jitter=0.0)


# -- child entry points, at module level so spawn can pickle them ---------


def child_enqueue(path: str, count: int, tag: str) -> None:
    """Enqueue ``count`` jobs tagged with this worker's name."""
    with Queue(path) as queue:
        for i in range(count):
            queue.enqueue({"tag": tag, "n": i})


def child_drain(path: str, out: str) -> None:
    """Lease and ack everything available, recording the ids taken."""
    taken: list[str] = []
    with Queue(path, visibility_timeout=30.0) as queue:
        while True:
            jobs = queue.lease(5)
            if not jobs:
                break
            for job in jobs:
                taken.append(job.id)
                queue.ack(job)
    Path(out).write_text(json.dumps(taken), encoding="utf-8")


class ThreadSafetyTest(TempQueueTest):
    """Threads share one Queue object; the internal lock has to hold."""

    def test_threads_never_receive_the_same_job(self) -> None:
        queue = Queue(self.dir, backoff=IMMEDIATE)
        self.addCleanup(queue.close)
        total = 400
        queue.enqueue_many([{"n": i} for i in range(total)])

        seen: list[str] = []
        seen_lock = threading.Lock()

        def drain() -> None:
            while True:
                jobs = queue.lease(7)
                if not jobs:
                    return
                with seen_lock:
                    seen.extend(job.id for job in jobs)
                for job in jobs:
                    queue.ack(job)

        threads = [threading.Thread(target=drain) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(seen), total)
        self.assertEqual(len(set(seen)), total, "a job was leased twice")
        self.assertEqual(queue.stats().done, total)

    def test_concurrent_enqueue_loses_nothing(self) -> None:
        queue = Queue(self.dir)
        self.addCleanup(queue.close)

        def produce(tag: int) -> None:
            for i in range(50):
                queue.enqueue({"tag": tag, "n": i})

        threads = [threading.Thread(target=produce, args=(t,)) for t in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(queue), 300)


class MultiProcessTest(TempQueueTest):
    """Separate OS processes, coordinating through nothing but the files."""

    def _spawn(self, target, *args) -> multiprocessing.Process:
        ctx = multiprocessing.get_context("spawn")
        process = ctx.Process(target=target, args=args)
        process.start()
        return process

    def _run_all(self, specs) -> None:
        processes = [self._spawn(target, *args) for target, args in specs]
        for process in processes:
            process.join(timeout=120)
            self.assertFalse(process.is_alive(), "a child process hung")
            self.assertEqual(process.exitcode, 0, "a child process failed")

    def test_producers_in_separate_processes_do_not_lose_writes(self) -> None:
        producers = 4
        per_producer = 40
        self._run_all(
            [
                (child_enqueue, (str(self.dir), per_producer, f"p{i}"))
                for i in range(producers)
            ]
        )

        with Queue(self.dir) as queue:
            jobs = queue.list_jobs()
        self.assertEqual(
            len(jobs),
            producers * per_producer,
            "appends from separate processes must all survive",
        )
        # Sequence numbers are allocated under the lock, so they must be unique
        # even though four processes were assigning them.
        self.assertEqual(len({job.seq for job in jobs}), len(jobs))

    def test_consumers_in_separate_processes_never_share_a_job(self) -> None:
        total = 200
        with Queue(self.dir) as queue:
            queue.enqueue_many([{"n": i} for i in range(total)])
            expected = {job.id for job in queue.list_jobs()}

        outputs = [self.dir / f"taken-{i}.json" for i in range(4)]
        self._run_all([(child_drain, (str(self.dir), str(out))) for out in outputs])

        taken: list[str] = []
        for out in outputs:
            taken.extend(json.loads(out.read_text(encoding="utf-8")))

        self.assertEqual(len(taken), len(set(taken)), "two processes got the same job")
        self.assertEqual(set(taken), expected, "some jobs were never delivered")

        with Queue(self.dir) as queue:
            self.assertEqual(queue.stats().done, total)

    def test_a_peer_sees_work_enqueued_after_it_opened(self) -> None:
        """No notification channel exists, so catching up must be automatic."""
        reader = Queue(self.dir)
        self.addCleanup(reader.close)
        self.assertEqual(len(reader), 0)

        self._run_all([(child_enqueue, (str(self.dir), 10, "late"))])

        # The reader was open the whole time and was never told anything.
        self.assertEqual(len(reader), 10)
        job = reader.lease_one()
        assert job is not None
        self.assertEqual(job.payload["tag"], "late")


class LockTest(TempQueueTest):
    def test_the_lock_is_exclusive(self) -> None:
        from nobroker.errors import LockTimeoutError
        from nobroker.lock import FileLock

        path = self.dir / "test.lock"
        held = FileLock(path)
        held.acquire()
        self.addCleanup(held.release)

        contender = FileLock(path, timeout=0.05)
        with self.assertRaises(LockTimeoutError):
            contender.acquire()

    def test_releasing_lets_the_next_holder_in(self) -> None:
        from nobroker.lock import FileLock

        path = self.dir / "test.lock"
        with FileLock(path):
            pass
        with FileLock(path, timeout=0.5):
            pass  # would raise if the first hold leaked

    def test_reentrant_acquire_is_a_bug_not_a_deadlock(self) -> None:
        from nobroker.lock import FileLock

        lock = FileLock(self.dir / "test.lock")
        lock.acquire()
        self.addCleanup(lock.release)
        with self.assertRaises(RuntimeError):
            lock.acquire()


if __name__ == "__main__":
    unittest.main()

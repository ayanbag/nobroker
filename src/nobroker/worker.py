"""A worker runner: poll, hand jobs to a callable, ack or nack the result.

This is the part users actually touch, so it is opinionated about the two things
people get wrong when they write it themselves.

**Shutdown.** Ctrl-C during a handler must not lose the job. The signal flips a
flag; in-flight handlers run to completion and ack normally; only then does the
process exit. Killing a worker should cost you nothing, so that deploying is
boring.

**Long handlers.** A job that takes longer than the visibility timeout would be
redelivered while it is still running -- duplicate work, from a worker that is
perfectly healthy. A heartbeat thread extends the lease of anything in flight, so
the timeout can stay short (fast recovery from real crashes) without punishing
slow jobs.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .errors import JobNotFoundError, NobrokerError, NotLeasedError
from .job import Job, JobState
from .queue import Queue

#: A handler receives the job and returns anything (ignored). Raising means fail.
Handler = Callable[[Job], Any]

log = logging.getLogger("nobroker.worker")


@dataclass
class WorkerStats:
    """Counters for one worker run."""

    leased: int = 0
    succeeded: int = 0
    failed: int = 0
    dead: int = 0
    lost_leases: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "leased": self.leased,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "dead": self.dead,
            "lost_leases": self.lost_leases,
        }


@dataclass
class Worker:
    """Runs a handler over a queue until told to stop.

    Args:
        queue: The queue to consume.
        handler: Called with each leased :class:`~nobroker.job.Job`. Returning
            normally acks; raising nacks and schedules a retry.
        concurrency: Handler threads. Threads, not processes, because handlers
            in a job queue are almost always I/O-bound -- and if yours is not,
            run several worker *processes*, which nobroker supports natively.
        poll_interval: Seconds to sleep when the queue is empty. There is no
            long-poll or notification channel: a broker-less queue has nobody to
            send you one. Polling an idle mmap-warm file is a cheap loop, and
            saying so is more honest than pretending otherwise.
        max_jobs: Stop after this many jobs. Useful in tests and for workers you
            want to recycle periodically.
        idle_timeout: Stop after this many seconds with nothing to do.
        handle_signals: Install SIGINT/SIGTERM handlers for graceful shutdown.
            Only possible from the main thread; ignored elsewhere.
    """

    queue: Queue
    handler: Handler
    concurrency: int = 1
    poll_interval: float = 0.1
    max_jobs: int | None = None
    idle_timeout: float | None = None
    handle_signals: bool = True
    stats: WorkerStats = field(default_factory=WorkerStats)

    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _counter_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _in_flight: dict[int, Job] = field(default_factory=dict, repr=False)

    def stop(self) -> None:
        """Ask the worker to finish in-flight jobs and return."""
        self._stop.set()

    def run(self) -> WorkerStats:
        """Consume the queue until stopped, drained, or capped. Blocks."""
        restore = self._install_signal_handlers()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop, name="nobroker-heartbeat", daemon=True
        )
        heartbeat.start()
        threads = [
            threading.Thread(target=self._loop, name=f"nobroker-worker-{i}", daemon=True)
            for i in range(self.concurrency)
        ]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            self._stop.set()
            heartbeat.join(timeout=1.0)
            restore()
        return self.stats

    # -- internals --------------------------------------------------------

    def _loop(self) -> None:
        idle_since: float | None = None
        while not self._stop.is_set():
            if self._reached_cap():
                return
            try:
                job = self.queue.lease_one()
            except NobrokerError:
                log.exception("lease failed; backing off")
                self._stop.wait(self.poll_interval)
                continue

            if job is None:
                now = time.monotonic()
                idle_since = now if idle_since is None else idle_since
                if (
                    self.idle_timeout is not None
                    and now - idle_since >= self.idle_timeout
                ):
                    return
                self._stop.wait(self.poll_interval)
                continue

            idle_since = None
            with self._counter_lock:
                self.stats.leased += 1
            self._run_one(job)

    def _run_one(self, job: Job) -> None:
        """Execute one job and record the outcome. Never raises.

        Two layers, and the outer one is the important half of the promise. The
        inner ``except`` treats a handler failure as data. The outer one covers
        recording the *outcome*, which can fail for reasons that have nothing to
        do with the handler -- the lock was busy past ``lock_timeout``, the disk
        filled, an operator purged the queue mid-flight.

        Letting that escape would end the thread, and a worker whose threads die
        one by one goes quiet without ever reporting that it stopped working. So
        it is logged and swallowed: the job is still leased, and the visibility
        timeout redelivers it, which is the recovery path the design already has.
        """
        ident = threading.get_ident()
        self._in_flight[ident] = job
        try:
            try:
                self.handler(job)
            except Exception as exc:  # handler failure is data, not a crash
                log.warning("job %s failed: %s", job.id, exc)
                self._fail(job, exc)
            else:
                self._succeed(job)
        except NobrokerError:
            log.exception(
                "could not record the outcome of job %s; it stays leased and "
                "will be redelivered when its lease expires",
                job.id,
            )
        finally:
            self._in_flight.pop(ident, None)

    def _succeed(self, job: Job) -> None:
        try:
            self.queue.ack(job)
        except JobNotFoundError:
            # The job stopped existing while the handler ran -- a purge is the
            # only way that happens. Not an error: there is nothing left to ack,
            # and the operator who purged knows the work was in flight.
            log.warning("job %s no longer exists; nothing to ack", job.id)
            return
        except NotLeasedError:
            # The lease expired mid-handler and the job was redelivered. The work
            # is done; someone else is doing it again. This is at-least-once
            # showing its teeth, and it is why handlers must be idempotent.
            log.warning("lost lease on %s before ack; work may run twice", job.id)
            with self._counter_lock:
                self.stats.lost_leases += 1
            return
        with self._counter_lock:
            self.stats.succeeded += 1

    def _fail(self, job: Job, exc: Exception) -> None:
        try:
            updated = self.queue.nack(job, error=f"{type(exc).__name__}: {exc}")
        except JobNotFoundError:
            log.warning("job %s no longer exists; nothing to retry", job.id)
            return
        except NotLeasedError:
            log.warning("lost lease on %s before nack", job.id)
            with self._counter_lock:
                self.stats.lost_leases += 1
            return
        with self._counter_lock:
            self.stats.failed += 1
            if updated.state is JobState.DEAD:
                self.stats.dead += 1
                log.error(
                    "job %s exhausted %d attempts and moved to the DLQ",
                    job.id,
                    updated.max_attempts,
                )

    def _heartbeat_loop(self) -> None:
        """Extend leases of jobs still running, so slow work is not redelivered.

        Fires at a third of the visibility timeout: often enough that two missed
        beats still leave margin, rare enough that it is not a write amplifier.
        """
        interval = max(self.queue.visibility_timeout / 3.0, 0.05)
        while not self._stop.wait(interval):
            for job in list(self._in_flight.values()):
                try:
                    self.queue.extend(job, self.queue.visibility_timeout)
                except NobrokerError:
                    # Already reclaimed, or the queue closed underneath us.
                    # _succeed/_fail will report it properly; nothing to do here.
                    pass

    def _reached_cap(self) -> bool:
        if self.max_jobs is None:
            return False
        with self._counter_lock:
            return self.stats.leased >= self.max_jobs

    def _install_signal_handlers(self) -> Callable[[], None]:
        """Wire SIGINT/SIGTERM to :meth:`stop`, returning an undo callable."""
        if not self.handle_signals or threading.current_thread() is not threading.main_thread():
            return lambda: None

        previous: dict[int, Any] = {}

        def on_signal(signum: int, _frame: Any) -> None:
            log.info("signal %s received; finishing in-flight jobs", signum)
            self.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous[sig] = signal.signal(sig, on_signal)
            except (OSError, ValueError):  # pragma: no cover - platform dependent
                pass

        def restore() -> None:
            for sig, handler in previous.items():
                try:
                    signal.signal(sig, handler)
                except (OSError, ValueError):  # pragma: no cover
                    pass

        return restore

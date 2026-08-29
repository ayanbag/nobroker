"""The in-memory view of the queue, rebuilt by replaying the log.

Two properties are worth stating up front because everything here is arranged
around them:

**Replay is a pure function of the log.** Nothing in :meth:`Index.apply` reads
the clock, calls ``random``, or generates an id. Every non-deterministic value --
the lease deadline, the jittered retry time, the job id -- is sampled once by the
writer and *recorded*. Replaying the same log twice therefore produces identical
state, which is what makes crash recovery testable rather than hopeful.

**Deletion is lazy.** ``heapq`` has no "remove this element". The documented
workaround is to leave stale entries in place and skip them on pop. Each entry
carries the job version it was pushed with; if the job has moved on, the entry is
garbage and is dropped when it surfaces. Cost is bounded by the number of state
changes, which the log bounds anyway.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Iterable

from .codec import Record, RecordType
from .job import Job, JobState

#: Heap entries: (sort key..., version, job_id). Version is what makes lazy
#: deletion work -- see the module docstring.
_ReadyEntry = tuple[int, float, int, int, str]
_ScheduledEntry = tuple[float, int, int, str]
_LeaseEntry = tuple[float, int, str]


@dataclass(frozen=True, slots=True)
class QueueStats:
    """A point-in-time count of jobs by state."""

    ready: int = 0
    delayed: int = 0
    leased: int = 0
    done: int = 0
    dead: int = 0
    total: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "ready": self.ready,
            "delayed": self.delayed,
            "leased": self.leased,
            "done": self.done,
            "dead": self.dead,
            "total": self.total,
        }


@dataclass
class Index:
    """Queue state derived from the log.

    Holds no file handles and performs no I/O, which is why it can be unit-tested
    by feeding it records directly and why the same :meth:`apply` runs for both
    live operations and recovery. One code path, exercised twice as hard.
    """

    jobs: dict[str, Job] = field(default_factory=dict)
    seq: int = 0
    orphan_records: int = 0

    _ready: list[_ReadyEntry] = field(default_factory=list, repr=False)
    _scheduled: list[_ScheduledEntry] = field(default_factory=list, repr=False)
    _leases: list[_LeaseEntry] = field(default_factory=list, repr=False)

    # -- record application ----------------------------------------------

    def apply(self, record: Record) -> None:
        """Fold one log record into the index.

        Records naming a job we have never seen are counted and ignored rather
        than raising. That situation means the log lost history it should not
        have -- worth reporting, but refusing to open a queue over it would turn
        a partial recovery into a total one.
        """
        handler = _HANDLERS.get(record.type)
        if handler is None:  # pragma: no cover - codec rejects unknown types
            self.orphan_records += 1
            return
        handler(self, record.payload)

    def _apply_enqueue(self, payload: dict[str, Any]) -> None:
        job = Job.from_dict(payload)
        self.seq = max(self.seq, job.seq)
        self.jobs[job.id] = job
        self._admit(job)

    def _apply_lease(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.LEASED
        job.attempts = int(payload["attempts"])
        job.lease_deadline = float(payload["deadline"])
        job.lease_token = payload.get("token")
        job.version += 1
        heapq.heappush(self._leases, (job.lease_deadline, job.version, job.id))

    def _apply_extend(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None or job.state is not JobState.LEASED:
            return
        job.lease_deadline = float(payload["deadline"])
        job.version += 1
        heapq.heappush(self._leases, (job.lease_deadline, job.version, job.id))

    def _apply_ack(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.DONE
        job.lease_deadline = None
        job.lease_token = None
        job.version += 1

    def _apply_nack(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.READY
        job.attempts = int(payload.get("attempts", job.attempts))
        job.available_at = float(payload["available_at"])
        job.last_error = payload.get("error")
        job.lease_deadline = None
        job.lease_token = None
        job.version += 1
        self._schedule(job)

    def _apply_reclaim(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.READY
        job.available_at = float(payload.get("available_at", job.available_at))
        job.lease_deadline = None
        job.lease_token = None
        job.version += 1
        self._schedule(job)

    def _apply_dead(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.DEAD
        job.lease_deadline = None
        job.lease_token = None
        job.last_error = payload.get("error", job.last_error)
        job.version += 1

    def _apply_requeue(self, payload: dict[str, Any]) -> None:
        job = self._get(payload["id"])
        if job is None:
            return
        job.state = JobState.READY
        job.attempts = 0
        job.available_at = float(payload.get("available_at", 0.0))
        job.max_attempts = int(payload.get("max_attempts", job.max_attempts))
        job.lease_deadline = None
        job.lease_token = None
        job.version += 1
        self._schedule(job)

    def _apply_purge(self, _payload: dict[str, Any]) -> None:
        # Signature matches the other handlers so dispatch stays uniform; a purge
        # carries no state beyond the fact that it happened.
        self.jobs.clear()
        self._ready.clear()
        self._scheduled.clear()
        self._leases.clear()

    # -- scheduling ------------------------------------------------------

    def _admit(self, job: Job) -> None:
        """Put a freshly materialised job onto whichever heap tracks its state.

        Needed because a compacted log stores a leased job as a single ENQUEUE
        record carrying ``state="leased"``. Without this, that job would rebuild
        with no entry on the lease heap and its visibility timeout would never
        fire -- it would be leased forever, invisible and unrecoverable.
        """
        if job.state is JobState.READY:
            self._schedule(job)
        elif job.state is JobState.LEASED and job.lease_deadline is not None:
            heapq.heappush(self._leases, (job.lease_deadline, job.version, job.id))

    def _schedule(self, job: Job) -> None:
        """Admit a READY job to the not-yet-eligible heap.

        Two heaps rather than one because the orderings genuinely differ.
        Eligible jobs sort by priority; pending jobs sort by the time they become
        eligible. Merging them would put a high-priority job scheduled for
        tomorrow at the head of the heap, blocking everything behind it -- the
        classic priority-queue-with-delays bug.

        Every job enters through the pending heap, even one with no delay, so
        that :meth:`apply` never has to ask what time it is. :meth:`promote` does
        the clock-reading, once per lease, and a job whose ``available_at`` is
        already in the past is promoted on the very next call.
        """
        heapq.heappush(
            self._scheduled, (job.available_at, job.seq, job.version, job.id)
        )

    def promote(self, now: float) -> int:
        """Move every job whose delay has elapsed onto the eligible heap.

        Returns the number promoted. Called at the top of every lease, which is
        the only moment the answer can matter.
        """
        moved = 0
        while self._scheduled and self._scheduled[0][0] <= now:
            available_at, seq, version, job_id = heapq.heappop(self._scheduled)
            job = self.jobs.get(job_id)
            if job is None or job.version != version or job.state is not JobState.READY:
                continue  # stale entry, superseded by a later state change
            heapq.heappush(
                self._ready, (-job.priority, available_at, seq, version, job_id)
            )
            moved += 1
        return moved

    def next_ready(self, now: float) -> Job | None:
        """Pop the highest-priority job that may be leased now, or None.

        Ordering is ``(priority desc, available_at asc, seq asc)``: priority
        first because that is what a priority is for, then oldest-first so a
        steady stream of same-priority work stays FIFO and nothing starves.
        """
        self.promote(now)
        while self._ready:
            neg_priority, available_at, seq, version, job_id = self._ready[0]
            job = self.jobs.get(job_id)
            if job is None or job.version != version or job.state is not JobState.READY:
                heapq.heappop(self._ready)
                continue
            if job.available_at > now:
                # Its delay was pushed back after this entry was queued; put it
                # on the scheduled heap where it belongs and keep looking.
                heapq.heappop(self._ready)
                heapq.heappush(
                    self._scheduled, (job.available_at, seq, version, job_id)
                )
                continue
            heapq.heappop(self._ready)
            return job
        return None

    def restore(self, job: Job) -> None:
        """Put a job popped by :meth:`next_ready` back on the eligible heap.

        :meth:`next_ready` pops before the lease is durable. If the write then
        fails, the job is still READY in ``jobs`` but on no heap at all -- it
        would go invisible until the next full replay. This puts it back.
        """
        self._schedule(job)

    def expired_leases(self, now: float) -> list[Job]:
        """Jobs whose visibility timeout has passed and which need reclaiming.

        A worker that dies holding a lease is indistinguishable from one that is
        merely slow -- so nobroker does not try to distinguish them. The deadline
        expires, the job becomes available again, and the handler had better be
        idempotent. That is at-least-once, stated plainly.
        """
        expired: list[Job] = []
        while self._leases and self._leases[0][0] <= now:
            deadline, version, job_id = heapq.heappop(self._leases)
            job = self.jobs.get(job_id)
            if job is None or job.version != version or job.state is not JobState.LEASED:
                continue
            expired.append(job)
        return expired

    # -- queries ----------------------------------------------------------

    def _get(self, job_id: str) -> Job | None:
        job = self.jobs.get(job_id)
        if job is None:
            self.orphan_records += 1
        return job

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def stats(self, now: float) -> QueueStats:
        """Count jobs by state. O(n) -- honest, and n is what fits in memory."""
        ready = delayed = leased = done = dead = 0
        for job in self.jobs.values():
            if job.state is JobState.READY:
                if job.available_at > now:
                    delayed += 1
                else:
                    ready += 1
            elif job.state is JobState.LEASED:
                leased += 1
            elif job.state is JobState.DONE:
                done += 1
            else:
                dead += 1
        return QueueStats(
            ready=ready,
            delayed=delayed,
            leased=leased,
            done=done,
            dead=dead,
            total=len(self.jobs),
        )

    def live_jobs(self) -> Iterable[Job]:
        """Jobs a compacted log must still contain.

        Completed jobs are dropped -- that is what makes compaction shrink
        anything. Dead-letter jobs are kept: the DLQ is a queryable record of
        what failed, and silently discarding it during maintenance would be a
        nasty surprise.
        """
        return (job for job in self.jobs.values() if job.state is not JobState.DONE)

    def retain(self, jobs: Iterable[Job]) -> None:
        """Replace the job table with ``jobs`` and rebuild every heap.

        Called after compaction, where the new log contains only live jobs. Left
        out, the in-memory index would still hold the completed jobs the log no
        longer mentions -- so ``stats()`` would disagree with the file, and the
        disagreement would vanish on restart, which is the worst kind of bug.
        """
        self.jobs = {job.id: job for job in jobs}
        self.rebuild_heaps()

    def rebuild_heaps(self) -> None:
        """Recreate the heaps from ``jobs``. Used after compaction."""
        self._ready.clear()
        self._scheduled.clear()
        self._leases.clear()
        for job in self.jobs.values():
            self._admit(job)


_HANDLERS = {
    RecordType.ENQUEUE: Index._apply_enqueue,
    RecordType.LEASE: Index._apply_lease,
    RecordType.ACK: Index._apply_ack,
    RecordType.NACK: Index._apply_nack,
    RecordType.RECLAIM: Index._apply_reclaim,
    RecordType.DEAD: Index._apply_dead,
    RecordType.REQUEUE: Index._apply_requeue,
    RecordType.PURGE: Index._apply_purge,
    RecordType.EXTEND: Index._apply_extend,
}

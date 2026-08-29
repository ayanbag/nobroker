"""The public queue: enqueue, lease, ack, nack, and the machinery behind them.

Every mutating operation follows the same four beats:

1. Take the cross-process file lock.
2. Catch up -- apply any records peers appended since we last looked.
3. Reclaim leases whose visibility timeout has expired.
4. Do the thing: append records, fsync, apply them locally.

Step 2 is what makes several *processes* safe against each other with no server
between them. The log is the source of truth; a process that has been idle
simply reads forward from its last offset and finds out what happened. No
heartbeats, no gossip, no coordinator -- which is the entire pitch.

Ordering inside step 4 matters and is not negotiable: the record reaches disk
*before* the in-memory state changes. Crash between the two and recovery replays
the record, arriving at the same state. Do it the other way round and a crash
loses an operation the caller was told had succeeded.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

from .backoff import BackoffPolicy
from .codec import (
    HEADER_SIZE,
    Damage,
    Record,
    RecordType,
    encode_file_header,
    encode_record,
)
from .errors import (
    JobNotFoundError,
    NotLeasedError,
    QueueClosedError,
    CompactionError,
)
from .index import Index, QueueStats
from .job import Job, JobState
from .lock import FileLock
from .logfile import O_BINARY, Layout, LogFile, RecoveryReport, fsync_directory

DEFAULT_VISIBILITY_TIMEOUT = 30.0
DEFAULT_MAX_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """What a compaction pass achieved."""

    bytes_before: int
    bytes_after: int
    jobs_kept: int
    jobs_dropped: int

    @property
    def bytes_reclaimed(self) -> int:
        return self.bytes_before - self.bytes_after

    def describe(self) -> str:
        pct = (
            100.0 * self.bytes_reclaimed / self.bytes_before
            if self.bytes_before
            else 0.0
        )
        return (
            f"compacted {self.bytes_before} -> {self.bytes_after} bytes "
            f"({pct:.1f}% reclaimed), kept {self.jobs_kept} jobs, "
            f"dropped {self.jobs_dropped} completed"
        )


class Queue:
    """A durable, broker-less job queue backed by one append-only log file.

    Safe to share between threads, and safe to open from as many processes as you
    like against the same directory.

    Example:
        >>> q = Queue("/var/lib/myapp/jobs")
        >>> q.enqueue({"send_email_to": "ada@example.com"})       # doctest: +SKIP
        >>> job = q.lease_one()                                    # doctest: +SKIP
        >>> q.ack(job)                                             # doctest: +SKIP

    Delivery is **at-least-once**. A job can be delivered more than once -- a
    worker that dies after finishing but before acking will see its job again,
    and no amount of engineering removes that window without a transaction that
    spans your handler's own datastore. Write idempotent handlers.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        name: str = "default",
        *,
        fsync: bool = True,
        visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff: BackoffPolicy | None = None,
        lock_timeout: float = 10.0,
    ) -> None:
        """Open (creating if needed) the queue ``name`` inside directory ``path``.

        Args:
            path: Directory holding the queue's files. Created if absent.
            name: Queue name. Each name is an independent log, so one directory
                can hold ``emails``, ``thumbnails`` and ``webhooks`` side by side
                without them contending for the same lock.
            fsync: Force each write to disk before returning. Turning this off
                makes writes roughly two orders of magnitude faster and gives up
                the central guarantee: a machine crash can then lose
                acknowledged work. It exists for benchmarks and for caches where
                loss is acceptable. Say so out loud if you use it.
            visibility_timeout: Seconds a leased job stays invisible before it is
                assumed lost and made available again.
            max_attempts: Deliveries allowed before a job is moved to the DLQ.
            backoff: Retry delay policy. See :class:`~nobroker.backoff.BackoffPolicy`.
            lock_timeout: Seconds to wait for the cross-process lock.
        """
        self.dir = Path(path)
        self.name = name
        self.fsync = fsync
        self.visibility_timeout = visibility_timeout
        self.max_attempts = max_attempts
        self.backoff = backoff or BackoffPolicy()

        self.dir.mkdir(parents=True, exist_ok=True)
        self._layout = Layout(self.dir, name)
        self._flock = FileLock(self._layout.lock, timeout=lock_timeout)
        self._tlock = threading.RLock()
        self._index = Index()
        self._read_pos = HEADER_SIZE
        self._recovery = RecoveryReport()
        self._closed = False
        self._pointer_key: tuple[int, int, int] | None = None
        self._pointer_generation = 0

        with self._flock:
            self._log = self._open_live_log()
            self._replay(full=True)

    def _open_live_log(self) -> LogFile:
        """Open whichever generation the pointer file says is authoritative."""
        generation = self._layout.read_generation()
        log = LogFile(self._layout.log(generation))
        log.open(generation=generation)
        return log

    # -- properties -------------------------------------------------------

    @property
    def recovery(self) -> RecoveryReport:
        """What the last full replay found. Check ``.repaired`` after a crash."""
        return self._recovery

    @property
    def path(self) -> Path:
        """Path of the log file backing this queue."""
        return self._log.path

    # -- core write path --------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[float]:
        """Hold both locks, catch up with peers, reap expired leases.

        Yields the wall-clock time the operation should consider "now", sampled
        once so that a single operation cannot see time move underneath it.
        """
        if self._closed:
            raise QueueClosedError(f"queue {self.name!r} is closed")
        with self._tlock:
            with self._flock:
                self._catch_up()
                now = time.time()
                self._reap(now)
                yield now

    def _catch_up(self) -> None:
        """Apply anything peers wrote since our last read, or re-read from scratch.

        The cheap path reads forward from ``_read_pos``. The expensive path -- a
        full re-replay -- runs only when the pointer file names a generation
        newer than the one we have open, which happens exactly when a peer
        compacted.
        """
        if self._live_generation() != self._log.generation:
            self._log.close()
            self._log = self._open_live_log()
            self._replay(full=True)
        else:
            self._replay(full=False)

    def _live_generation(self) -> int:
        """Which generation is authoritative, checked as cheaply as possible.

        Every operation has to ask, because a peer may have compacted since we
        last looked. Reading the pointer file costs an open, a read and a close;
        stat'ing it costs one syscall. Since compaction replaces the pointer
        atomically, the replacement always arrives as a *different file* -- new
        identity, new mtime, and usually a new size -- so an unchanged stat is
        proof the answer has not changed.

        The cache is skipped entirely on filesystems that do not report inode
        numbers, where the identity half of that argument does not hold.
        """
        try:
            st = os.stat(self._layout.pointer)
        except OSError:
            self._pointer_key = None
            return self._layout.read_generation()

        key = (st.st_ino, st.st_mtime_ns, st.st_size)
        if st.st_ino and key == self._pointer_key:
            return self._pointer_generation

        self._pointer_generation = self._layout.read_generation()
        self._pointer_key = key
        return self._pointer_generation

    def _replay(self, *, full: bool) -> None:
        """Read records from the log into the index, repairing a torn tail.

        Repair is a truncation back to the last record that checksummed cleanly.
        Everything after it is, by definition, a write that had not completed
        when the process died -- an operation whose caller never got a return
        value, and which therefore never happened.
        """
        if full:
            self._index = Index()
            start = HEADER_SIZE
        else:
            start = self._read_pos

        records, valid_end, damage = self._log.scan(start)
        for record in records:
            self._index.apply(record)

        discarded = 0
        if damage is not Damage.NONE:
            discarded = self._log.truncate(valid_end)
        self._read_pos = valid_end

        report = RecoveryReport(
            records_applied=len(records),
            bytes_scanned=valid_end - start,
            bytes_discarded=discarded,
            damage=damage,
            orphan_records=self._index.orphan_records,
        )
        if full or report.repaired:
            self._recovery = report

    def _commit(
        self,
        records: Sequence[tuple[RecordType, dict[str, Any]]],
        *,
        fsync: bool | None = None,
    ) -> None:
        """Durably append records, then apply them to the in-memory index.

        Disk first, memory second. See the module docstring for why the order is
        the whole ballgame.
        """
        if not records:
            return
        offset = self._log.end_offset
        self._log.append(records, fsync=self.fsync if fsync is None else fsync)
        for rtype, payload in records:
            self._index.apply(Record(offset, rtype, payload))
        self._read_pos = self._log.end_offset

    def _reap(self, now: float) -> int:
        """Return timed-out leases to the queue, or bury them in the DLQ.

        A lease that expires is treated exactly like a nack, backoff included. A
        worker that has gone quiet past its deadline is either dead or wedged,
        and both deserve the same "wait a bit before trying again" treatment as
        an explicit failure. It also stops a crash-looping worker from spinning
        the same job at full speed.
        """
        expired = self._index.expired_leases(now)
        if not expired:
            return 0
        records: list[tuple[RecordType, dict[str, Any]]] = []
        for job in expired:
            reason = f"lease expired after {self.visibility_timeout:g}s"
            if job.attempts >= job.max_attempts:
                records.append(
                    (RecordType.DEAD, {"id": job.id, "at": now, "error": reason})
                )
            else:
                delay = self.backoff.delay_for(job.attempts)
                records.append(
                    (
                        RecordType.RECLAIM,
                        {"id": job.id, "at": now, "available_at": now + delay},
                    )
                )
        self._commit(records)
        return len(records)

    # -- public API -------------------------------------------------------

    def enqueue(
        self,
        payload: Any,
        *,
        priority: int = 0,
        delay: float = 0.0,
        max_attempts: int | None = None,
        job_id: str | None = None,
    ) -> Job:
        """Add one job and return it. Durable by the time this returns.

        Args:
            payload: Any JSON-serialisable value.
            priority: Higher leases first. Ties break FIFO.
            delay: Seconds before the job becomes eligible.
            max_attempts: Override the queue default for this job.
            job_id: Supply your own id to make enqueueing idempotent. Enqueueing
                an id that already exists is a no-op that returns the existing
                job -- so a retried HTTP request that re-enqueues with the same
                key does not double-schedule the work. This is the one place
                nobroker can offer exactly-once semantics honestly, because
                de-duplication on a key is a thing a log *can* do.

        Returns:
            The stored job, as a detached snapshot.
        """
        return self.enqueue_many(
            [payload],
            priority=priority,
            delay=delay,
            max_attempts=max_attempts,
            job_ids=None if job_id is None else [job_id],
        )[0]

    def enqueue_many(
        self,
        payloads: Sequence[Any],
        *,
        priority: int = 0,
        delay: float = 0.0,
        max_attempts: int | None = None,
        job_ids: Sequence[str] | None = None,
    ) -> list[Job]:
        """Add many jobs with one write and one fsync.

        This is where batching pays: fsync costs the same for one record as for a
        thousand, so bulk enqueue is roughly N times faster per job than N calls
        to :meth:`enqueue`. The batch is still atomic in the way that matters --
        a torn write is detected and discarded whole by recovery.
        """
        if job_ids is not None and len(job_ids) != len(payloads):
            raise ValueError("job_ids must be the same length as payloads")

        with self._transaction() as now:
            records: list[tuple[RecordType, dict[str, Any]]] = []
            created: list[Job] = []
            for i, payload in enumerate(payloads):
                job_id = job_ids[i] if job_ids is not None else uuid.uuid4().hex
                existing = self._index.jobs.get(job_id)
                if existing is not None:
                    created.append(existing.snapshot())
                    continue
                job = Job(
                    id=job_id,
                    payload=payload,
                    priority=priority,
                    state=JobState.READY,
                    attempts=0,
                    max_attempts=(
                        self.max_attempts if max_attempts is None else max_attempts
                    ),
                    available_at=now + delay,
                    enqueued_at=now,
                    seq=self._index.next_seq(),
                )
                records.append((RecordType.ENQUEUE, job.to_dict()))
                created.append(job)
            self._commit(records)
            return created

    def lease(
        self, count: int = 1, *, visibility_timeout: float | None = None
    ) -> list[Job]:
        """Take up to ``count`` eligible jobs and hold them invisibly.

        Returns fewer than ``count`` -- possibly zero -- when the queue is short
        of eligible work. It never blocks; polling policy belongs to the caller,
        and :class:`~nobroker.worker.Worker` implements one.

        Each returned job carries a ``lease_token``. Pass the job object back to
        :meth:`ack` or :meth:`nack` and the token is checked, so a worker whose
        lease quietly expired cannot complete a delivery that now belongs to
        somebody else.
        """
        if count < 1:
            raise ValueError("count must be >= 1")
        timeout = (
            self.visibility_timeout if visibility_timeout is None else visibility_timeout
        )

        with self._transaction() as now:
            taken: list[Job] = []
            records: list[tuple[RecordType, dict[str, Any]]] = []
            try:
                while len(taken) < count:
                    job = self._index.next_ready(now)
                    if job is None:
                        break
                    token = uuid.uuid4().hex[:16]
                    records.append(
                        (
                            RecordType.LEASE,
                            {
                                "id": job.id,
                                "at": now,
                                "deadline": now + timeout,
                                "attempts": job.attempts + 1,
                                "token": token,
                            },
                        )
                    )
                    taken.append(job)
            except BaseException:
                # next_ready() pops from the heap; if we bail before committing,
                # those jobs would be invisible until the next full replay.
                for job in taken:
                    self._index.restore(job)
                raise
            self._commit(records)
            return [self._index.jobs[job.id].snapshot() for job in taken]

    def lease_one(self, *, visibility_timeout: float | None = None) -> Job | None:
        """Lease a single job, or return None if nothing is eligible."""
        jobs = self.lease(1, visibility_timeout=visibility_timeout)
        return jobs[0] if jobs else None

    def ack(self, job: Job | str) -> None:
        """Mark a leased job complete.

        Raises:
            NotLeasedError: The lease is no longer yours -- it expired and the
                job was reclaimed or redelivered. Your handler's side effects
                already happened, which is exactly the at-least-once window; the
                fix is an idempotent handler, not a retry here.
            JobNotFoundError: No such job.
        """
        with self._transaction() as now:
            live = self._require(job)
            if live.state is JobState.DONE:
                return  # already acked; acking twice is not an error
            self._require_lease(job, live)
            self._commit([(RecordType.ACK, {"id": live.id, "at": now})])

    def nack(
        self,
        job: Job | str,
        *,
        error: str | None = None,
        delay: float | None = None,
    ) -> Job:
        """Report a leased job as failed and schedule its retry.

        The job returns to the queue after a backoff delay, unless it has used up
        ``max_attempts``, in which case it moves to the dead-letter queue.

        Args:
            job: The leased job, or its id.
            error: Free text stored on the job for post-mortems.
            delay: Override the computed backoff, in seconds.

        Returns:
            The job in its new state -- inspect ``.state`` to see whether this
            nack was the one that killed it.
        """
        with self._transaction() as now:
            live = self._require(job)
            self._require_lease(job, live)

            if live.attempts >= live.max_attempts:
                self._commit(
                    [(RecordType.DEAD, {"id": live.id, "at": now, "error": error})]
                )
            else:
                wait = (
                    self.backoff.delay_for(live.attempts) if delay is None else delay
                )
                self._commit(
                    [
                        (
                            RecordType.NACK,
                            {
                                "id": live.id,
                                "at": now,
                                "attempts": live.attempts,
                                "available_at": now + wait,
                                "error": error,
                            },
                        )
                    ]
                )
            return self._index.jobs[live.id].snapshot()

    def extend(self, job: Job | str, seconds: float) -> Job:
        """Push a lease deadline further out, for a handler that needs longer.

        The alternative -- setting a generous visibility timeout for everyone --
        means a crashed worker's jobs stay invisible for that long too. Extending
        keeps the common case fast to recover and lets the slow case say so.
        """
        with self._transaction() as now:
            live = self._require(job)
            self._require_lease(job, live)
            deadline = max(live.lease_deadline or now, now) + seconds
            self._commit(
                [(RecordType.EXTEND, {"id": live.id, "at": now, "deadline": deadline})]
            )
            return self._index.jobs[live.id].snapshot()

    def requeue_dead(self, job_id: str | None = None) -> int:
        """Move dead-letter jobs back to READY with their attempt count reset.

        Args:
            job_id: One job, or ``None`` to revive the entire DLQ -- what you run
                after fixing the bug that killed them.

        Returns:
            How many jobs were revived.
        """
        with self._transaction() as now:
            if job_id is not None:
                live = self._require(job_id)
                if live.state is not JobState.DEAD:
                    raise NotLeasedError(f"job {job_id} is not in the dead-letter queue")
                targets = [live]
            else:
                targets = [
                    j for j in self._index.jobs.values() if j.state is JobState.DEAD
                ]
            self._commit(
                [
                    (
                        RecordType.REQUEUE,
                        {"id": j.id, "at": now, "available_at": now},
                    )
                    for j in targets
                ]
            )
            return len(targets)

    def purge(self) -> int:
        """Delete every job. Returns how many were removed.

        Recorded as a single PURGE record rather than by rewriting the file, so
        it is as durable and as crash-safe as any other operation.
        """
        with self._transaction() as now:
            count = len(self._index.jobs)
            self._commit([(RecordType.PURGE, {"at": now})])
            return count

    # -- reads ------------------------------------------------------------

    def get(self, job_id: str) -> Job:
        """Fetch a job by id."""
        with self._transaction():
            return self._require(job_id).snapshot()

    def stats(self) -> QueueStats:
        """Count jobs by state, after catching up with peers."""
        with self._transaction() as now:
            return self._index.stats(now)

    def list_jobs(
        self, state: JobState | str | None = None, *, limit: int | None = None
    ) -> list[Job]:
        """List jobs, newest last, optionally filtered by state."""
        wanted = JobState(state) if isinstance(state, str) else state
        with self._transaction():
            jobs = [
                job.snapshot()
                for job in self._index.jobs.values()
                if wanted is None or job.state is wanted
            ]
        jobs.sort(key=lambda j: j.seq)
        return jobs if limit is None else jobs[:limit]

    def dlq(self, *, limit: int | None = None) -> list[Job]:
        """List dead-lettered jobs."""
        return self.list_jobs(JobState.DEAD, limit=limit)

    def __len__(self) -> int:
        with self._transaction():
            return len(self._index.jobs)

    # -- maintenance ------------------------------------------------------

    def compact(self) -> CompactionResult:
        """Rewrite the log with only live jobs, dropping completed history.

        The log is append-only, so a queue that has processed a million jobs has
        a million records describing work that is finished and irrelevant.
        Compaction writes a *new generation*: a fresh log containing one ENQUEUE
        record per live job -- a snapshot in exactly the format it replaces, so
        there is no second reader to keep correct -- and then flips the pointer
        file to name it.

        The pointer flip is the commit point, and it is one atomic
        :func:`os.replace` of a file nobody holds open. Before it, the old
        generation is authoritative and the new file is ignorable garbage; after
        it, the reverse. There is no instant at which a reader could follow a
        half-written log. See :class:`~nobroker.logfile.Layout` for why the log
        is versioned rather than overwritten in place.

        Peers notice on their next operation: the pointer names a generation
        newer than the one they hold, so they reopen and re-replay.

        Raises:
            CompactionError: The new generation could not be written. The pointer
                still names the old log, which is untouched and fully usable.
        """
        with self._transaction():
            before = self._log.end_offset
            live = list(self._index.live_jobs())
            dropped = len(self._index.jobs) - len(live)
            generation = self._log.generation + 1
            target = self._layout.log(generation)

            blob = encode_file_header(generation) + b"".join(
                encode_record(RecordType.ENQUEUE, job.to_dict()) for job in live
            )
            tmp = self.dir / f"{self.name}.compact.{os.getpid()}"
            try:
                fd = os.open(
                    tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | O_BINARY, 0o644
                )
                try:
                    written = 0
                    while written < len(blob):
                        written += os.write(fd, blob[written:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                # The destination generation does not exist yet, so this replace
                # can never collide with a file somebody has open.
                os.replace(tmp, target)
                fsync_directory(self.dir)
                self._layout.write_generation(generation)  # commit point
            except OSError as exc:
                tmp.unlink(missing_ok=True)
                target.unlink(missing_ok=True)
                raise CompactionError(
                    f"could not write generation {generation}: {exc}. The queue "
                    "still points at the previous log and is unaffected."
                ) from exc

            old = self._log
            self._log = LogFile(target)
            self._log.open(generation=generation)
            old.close()
            self._read_pos = self._log.end_offset
            self._index.retain(live)
            self._sweep_stale_logs(generation)
            return CompactionResult(
                bytes_before=before,
                bytes_after=self._log.end_offset,
                jobs_kept=len(live),
                jobs_dropped=dropped,
            )

    def _sweep_stale_logs(self, live_generation: int) -> None:
        """Delete retired generations, ignoring any a peer still has open.

        Best-effort on purpose. A leftover ``queue.000003.log`` costs disk and
        nothing else -- the pointer file decides what is authoritative, so a
        stale generation is inert. The next compaction tries again.
        """
        for path in self._layout.stale_logs(live_generation):
            try:
                path.unlink()
            except OSError:
                pass

    def close(self) -> None:
        """Release the log descriptor. Idempotent."""
        with self._tlock:
            if self._closed:
                return
            self._closed = True
            self._log.close()
            self._flock.close()

    def __enter__(self) -> "Queue":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    # -- helpers ----------------------------------------------------------

    def _require(self, job: Job | str) -> Job:
        job_id = job if isinstance(job, str) else job.id
        live = self._index.jobs.get(job_id)
        if live is None:
            raise JobNotFoundError(f"no job {job_id!r} in queue {self.name!r}")
        return live

    def _require_lease(self, given: Job | str, live: Job) -> None:
        """Verify the caller still holds the lease it is trying to complete.

        Passing a bare id skips the token check. That is deliberate: the CLI and
        an operator poking at a stuck queue have no token to offer, and refusing
        them would make the tool useless exactly when you need it. Worker code
        passes the :class:`Job` object and gets fenced.
        """
        if live.state is not JobState.LEASED:
            raise NotLeasedError(
                f"job {live.id!r} is {live.state.value}, not leased; "
                "its visibility timeout most likely expired"
            )
        if isinstance(given, Job) and given.lease_token is not None:
            if given.lease_token != live.lease_token:
                raise NotLeasedError(
                    f"lease on job {live.id!r} was reclaimed and redelivered; "
                    "your token is stale"
                )

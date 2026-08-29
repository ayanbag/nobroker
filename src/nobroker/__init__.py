"""nobroker -- the job queue with nobody in the middle.

A durable, broker-less job queue for Python. Enqueue work, lease it, ack it.
Survives crashes. No Redis, no RabbitMQ, no server, no dependencies.

    from nobroker import Queue, Worker

    q = Queue("./jobs")
    q.enqueue({"greet": "world"})

    Worker(q, lambda job: print(job.payload), max_jobs=1).run()

Delivery is **at-least-once**: a job may be delivered more than once, so
handlers must be idempotent. See the README for why nothing honest can promise
otherwise.
"""

from .backoff import BackoffPolicy
from .errors import (
    CompactionError,
    CorruptLogError,
    JobNotFoundError,
    LockTimeoutError,
    NobrokerError,
    NotLeasedError,
    QueueClosedError,
    SerializationError,
)
from .index import QueueStats
from .job import Job, JobState
from .logfile import RecoveryReport
from .queue import CompactionResult, Queue
from .worker import Worker, WorkerStats

__version__ = "0.1.0"

__all__ = [
    "BackoffPolicy",
    "CompactionError",
    "CompactionResult",
    "CorruptLogError",
    "Job",
    "JobNotFoundError",
    "JobState",
    "LockTimeoutError",
    "NobrokerError",
    "NotLeasedError",
    "Queue",
    "QueueClosedError",
    "QueueStats",
    "RecoveryReport",
    "SerializationError",
    "Worker",
    "WorkerStats",
    "__version__",
]

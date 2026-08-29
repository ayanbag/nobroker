"""Handlers you can point a real worker at from the command line.

``nobroker work`` takes a ``module:callable`` spec and resolves it against the
current directory, so from a clone::

    nobroker --dir ./q enqueue '{"report": "q3"}'
    nobroker --dir ./q work examples.handlers:slow

These exist so the CLI's worker is demonstrable without you having to write an
application first -- and because a handler is the one piece of a job queue that
is always *your* code, so an example of the contract is worth more than prose.

The contract: a handler takes one ``Job`` and returns anything (the value is
discarded). **Returning means success and the job is acked. Raising means
failure**, and the job is nacked -- retried with backoff, or sent to the
dead-letter queue once ``max_attempts`` is spent.
"""

from __future__ import annotations

import random
import time
from typing import Any

from nobroker import Job


def echo(job: Job) -> None:
    """Succeed immediately, printing the payload. The simplest possible handler."""
    print(f"  handled {job.id[:8]}  {job.payload}")


def slow(job: Job) -> None:
    """Take 30 seconds, so there is time to kill the worker mid-job.

    Used by the crash demo: start a worker on this, `kill -9` it while the job is
    leased, and watch the lease expire and the job come back on its own.

    The 30s is chosen to match the default visibility timeout, which makes the
    lease heartbeat observable: a handler that runs exactly as long as its lease
    would be redelivered while still running, and isn't, because ``Worker``
    extends the lease every ``visibility_timeout / 3`` seconds. Kill the worker
    and the extensions stop, which is the only reason the lease then expires.
    """
    print(f"  working on {job.id[:8]} for 30s -- kill me (pid is in `pgrep -f nobroker`)")
    time.sleep(30)
    print(f"  finished {job.id[:8]}")


def flaky(job: Job) -> None:
    """Fail about half the time, to show retries, backoff and the DLQ.

    Nothing here is special-cased by nobroker: an ordinary exception is the whole
    failure protocol.
    """
    if random.random() < 0.5:
        raise RuntimeError("the upstream service fell over")
    print(f"  handled {job.id[:8]}  {job.payload}")


def counted(job: Job) -> Any:
    """Report which attempt this is -- the field that drives backoff and the DLQ.

    Enqueue with ``--max-attempts 3`` and run this to watch a job walk from
    attempt 1 to the dead-letter queue.
    """
    print(f"  {job.id[:8]}  attempt {job.attempts} of {job.max_attempts}")
    raise RuntimeError("failing on purpose")

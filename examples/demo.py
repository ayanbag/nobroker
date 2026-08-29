#!/usr/bin/env python3
"""An end-to-end tour of nobroker. Run it with ``make demo``.

Everything here is real: a real log file on disk, a real worker, a real crash.
Nothing is stubbed, and the queue directory is left behind afterwards so you can
poke at it with ``nobroker inspect``.
"""

from __future__ import annotations

import os
import random
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from nobroker import BackoffPolicy, Job, Queue, Worker  # noqa: E402


def rule(title: str) -> None:
    print(f"\n\033[1m{title}\033[0m")
    print("-" * max(len(title), 40))


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="nobroker-demo-"))
    print(f"queue directory: {root}")

    # ---------------------------------------------------------------- basics
    rule("1. Enqueue and lease")
    queue = Queue(root, "emails", visibility_timeout=5.0)
    for address in ["ada@example.com", "grace@example.com", "alan@example.com"]:
        job = queue.enqueue({"to": address, "template": "welcome"})
        print(f"  enqueued {job.id[:8]}  {address}")

    leased = queue.lease(2)
    print(f"  leased {len(leased)} jobs; the third is still visible to others")
    for job in leased:
        queue.ack(job)
    print(f"  acked both -> {queue.stats().to_dict()}")

    # ------------------------------------------------------------- priority
    rule("2. Priorities and delays")
    queue.enqueue({"to": "urgent@example.com"}, priority=10)
    queue.enqueue({"to": "later@example.com"}, delay=3600)
    order = [job.payload["to"] for job in queue.lease(5)]
    print(f"  lease order: {order}")
    print("  the delayed job is not in that list, and did not block the others")
    print(f"  {queue.stats().delayed} job is waiting out its delay")

    # -------------------------------------------------------------- retries
    rule("3. Retries, backoff and the dead-letter queue")
    retries = Queue(
        root,
        "flaky",
        max_attempts=3,
        backoff=BackoffPolicy(base=0.0, jitter=0.0),  # no waiting, for the demo
    )
    retries.enqueue({"charge": 4999, "card": "4242"})
    while True:
        job = retries.lease_one()
        if job is None:
            break
        result = retries.nack(job, error="payment gateway timeout")
        print(f"  attempt {result.attempts}/{result.max_attempts} -> {result.state.value}")
    dead = retries.dlq()
    print(f"  dead-letter queue holds {len(dead)}: {dead[0].last_error}")
    print(f"  requeued {retries.requeue_dead()} after 'fixing the bug'")

    # --------------------------------------------------------------- worker
    rule("4. A worker with a real handler")
    work = Queue(root, "thumbnails", backoff=BackoffPolicy(base=0.0, jitter=0.0))
    work.enqueue_many([{"image": f"photo-{i}.jpg"} for i in range(8)])

    processed: list[str] = []

    def handle(job: Job) -> None:
        if random.random() < 0.25:
            raise RuntimeError("imagemagick fell over")
        processed.append(job.payload["image"])

    random.seed(7)  # so the demo tells the same story every time
    stats = Worker(work, handle, concurrency=3, idle_timeout=0.0).run()
    print(f"  {stats.to_dict()}")
    print(f"  {len(processed)} images processed; failures were retried automatically")

    # ---------------------------------------------------------------- crash
    rule("5. Surviving a crash")
    crash_dir = root / "crashy"
    if hasattr(os, "fork"):
        survivors = _crash_with_fork(crash_dir)
    else:
        survivors = _crash_with_truncation(crash_dir)
    print(f"  after the crash, {survivors} jobs are still there and leasable")

    # ------------------------------------------------------------ inspection
    rule("6. The log is readable")
    log = Queue(root, "emails")
    for record in list(log._log.iter_all())[:6]:
        payload = str(record.payload)
        print(f"  @{record.offset:<6} {record.type.name:<8} {payload[:70]}")
    print("  ...")
    print(f"\n  try:  python -m nobroker.cli --dir {root} --queue emails inspect")
    print(f"        python -m nobroker.cli --dir {root} --queue emails stats")

    # ----------------------------------------------------------- compaction
    rule("7. Compaction")
    before = work.path.stat().st_size
    result = work.compact()
    print(f"  {result.describe()}")
    print(f"  log went from {before} to {work.path.stat().st_size} bytes")

    for q in (queue, retries, work, log):
        q.close()
    print(f"\nqueue directory left at {root} -- delete it when you are done.")
    return 0


def _crash_with_fork(path: Path) -> int:
    """Hard-kill a child mid-flight using fork, the POSIX way."""
    queue = Queue(path)
    queue.enqueue_many([{"n": i} for i in range(5)])
    queue.close()

    pid = os.fork()
    if pid == 0:
        child = Queue(path)
        child.lease(3)
        for i in range(20):
            child.enqueue({"extra": i})
        os._exit(9)  # no unwinding, no flush, no cleanup
    os.waitpid(pid, 0)
    print("  child process killed with os._exit(9) while holding leases")

    recovered = Queue(path)
    print(f"  recovery: {recovered.recovery.describe()}")
    total = recovered.stats().total
    recovered.close()
    return total


def _crash_with_truncation(path: Path) -> int:
    """Simulate a torn write directly, for platforms without fork.

    Chopping bytes off the end of the log is exactly what a crash mid-``write``
    leaves behind, so this exercises the same recovery path.
    """
    queue = Queue(path)
    queue.enqueue_many([{"n": i} for i in range(5)])
    log_path = queue.path
    queue.close()

    blob = log_path.read_bytes()
    log_path.write_bytes(blob[: len(blob) - 25] + b"\x00\x11\x22")
    print("  log truncated mid-record and padded with garbage")

    recovered = Queue(path)
    print(f"  recovery: {recovered.recovery.describe()}")
    total = recovered.stats().total
    recovered.close()
    return total


if __name__ == "__main__":
    raise SystemExit(main())

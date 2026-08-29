"""Command-line interface.

A queue you cannot inspect from a terminal is a queue you cannot operate. This
exists so that at 3am you can answer "what is stuck, why, and can I retry it"
without writing a script -- and so that ``nobroker inspect`` can show you the raw
log, because the whole durability story is only believable if you can read it.

Everything is :mod:`argparse`: subparsers, ``--json`` output, exit codes. No
third-party CLI framework, and none missed.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from typing import Any, Callable, Sequence

from . import __version__
from .errors import NobrokerError
from .job import Job, JobState
from .queue import Queue

DEFAULT_DIR = "./.nobroker"


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns a process exit code rather than calling sys.exit.

    Returning the code keeps the whole CLI testable in-process, which matters
    here: shelling out to test our own CLI would mean spawning interpreters in
    the test suite for no reason.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    if args.command is None:
        parser.print_help()
        return 2
    try:
        return args.func(args)
    except NobrokerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


# -- argument parsing -----------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nobroker",
        description="A durable, broker-less job queue. No server, no dependencies.",
    )
    parser.add_argument("--version", action="version", version=f"nobroker {__version__}")
    parser.add_argument(
        "-d", "--dir", default=DEFAULT_DIR, help=f"queue directory (default {DEFAULT_DIR})"
    )
    parser.add_argument("-q", "--queue", default="default", help="queue name")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")

    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("enqueue", help="add a job")
    p.add_argument("payload", help="JSON value, or plain text if it does not parse")
    p.add_argument("--priority", type=int, default=0, help="higher leases first")
    p.add_argument("--delay", type=float, default=0.0, help="seconds before eligible")
    p.add_argument("--max-attempts", type=int, default=None)
    p.add_argument("--id", dest="job_id", default=None, help="caller-supplied id (dedupes)")
    p.add_argument("--count", type=int, default=1, help="enqueue this many copies")
    p.set_defaults(func=_cmd_enqueue)

    p = sub.add_parser("lease", help="lease jobs and print them")
    p.add_argument("-n", "--count", type=int, default=1)
    p.add_argument("--timeout", type=float, default=None, help="visibility timeout")
    p.set_defaults(func=_cmd_lease)

    p = sub.add_parser("ack", help="mark a job complete")
    p.add_argument("job_id")
    p.set_defaults(func=_cmd_ack)

    p = sub.add_parser("nack", help="mark a job failed and schedule a retry")
    p.add_argument("job_id")
    p.add_argument("--error", default="nacked from the CLI")
    p.add_argument("--delay", type=float, default=None, help="override backoff")
    p.set_defaults(func=_cmd_nack)

    p = sub.add_parser("stats", help="count jobs by state")
    p.set_defaults(func=_cmd_stats)

    p = sub.add_parser("list", help="list jobs")
    p.add_argument("--state", choices=[s.value for s in JobState], default=None)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=_cmd_list)

    p = sub.add_parser("dlq", help="show or revive the dead-letter queue")
    p.add_argument("--requeue", nargs="?", const="__all__", default=None,
                   metavar="JOB_ID", help="revive one job, or all if no id is given")
    p.set_defaults(func=_cmd_dlq)

    p = sub.add_parser("purge", help="delete every job")
    p.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    p.set_defaults(func=_cmd_purge)

    p = sub.add_parser("compact", help="rewrite the log without completed jobs")
    p.set_defaults(func=_cmd_compact)

    p = sub.add_parser("inspect", help="dump raw log records")
    p.add_argument("--limit", type=int, default=None)
    p.set_defaults(func=_cmd_inspect)

    p = sub.add_parser("recover", help="open the queue and report on recovery")
    p.set_defaults(func=_cmd_recover)

    p = sub.add_parser("work", help="run a worker over the queue")
    p.add_argument("handler", help="import path, e.g. mypkg.tasks:handle")
    p.add_argument("-c", "--concurrency", type=int, default=1)
    p.add_argument("--max-jobs", type=int, default=None)
    p.add_argument("--idle-timeout", type=float, default=None)
    p.add_argument("--poll-interval", type=float, default=0.1)
    p.set_defaults(func=_cmd_work)

    p = sub.add_parser("bench", help="measure throughput on this machine")
    p.add_argument("-n", "--jobs", type=int, default=5000)
    p.add_argument("--markdown", action="store_true", help="emit a README table")
    p.set_defaults(func=_cmd_bench)

    return parser


# -- commands -------------------------------------------------------------


def _open(args: argparse.Namespace, **kwargs: Any) -> Queue:
    return Queue(args.dir, args.queue, **kwargs)


def _cmd_enqueue(args: argparse.Namespace) -> int:
    payload = _parse_payload(args.payload)
    with _open(args) as q:
        if args.count == 1:
            jobs = [
                q.enqueue(
                    payload,
                    priority=args.priority,
                    delay=args.delay,
                    max_attempts=args.max_attempts,
                    job_id=args.job_id,
                )
            ]
        else:
            jobs = q.enqueue_many(
                [payload] * args.count,
                priority=args.priority,
                delay=args.delay,
                max_attempts=args.max_attempts,
            )
    _emit(args, [j.to_dict() for j in jobs], lambda: "\n".join(j.id for j in jobs))
    return 0


def _cmd_lease(args: argparse.Namespace) -> int:
    with _open(args) as q:
        jobs = q.lease(args.count, visibility_timeout=args.timeout)
    if not jobs:
        _emit(args, [], lambda: "no eligible jobs")
        return 0
    _emit(args, [j.to_dict() for j in jobs], lambda: _format_jobs(jobs))
    return 0


def _cmd_ack(args: argparse.Namespace) -> int:
    with _open(args) as q:
        q.ack(args.job_id)
    _emit(args, {"acked": args.job_id}, lambda: f"acked {args.job_id}")
    return 0


def _cmd_nack(args: argparse.Namespace) -> int:
    with _open(args) as q:
        job = q.nack(args.job_id, error=args.error, delay=args.delay)
    _emit(
        args,
        job.to_dict(),
        lambda: f"nacked {job.id}: now {job.state.value}, attempt {job.attempts}/{job.max_attempts}",
    )
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    with _open(args) as q:
        stats = q.stats()
        size = q.path.stat().st_size
    data = stats.to_dict() | {"log_bytes": size}
    _emit(
        args,
        data,
        lambda: "\n".join(f"{k:>10}: {v}" for k, v in data.items()),
    )
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    with _open(args) as q:
        jobs = q.list_jobs(args.state, limit=args.limit)
    _emit(args, [j.to_dict() for j in jobs], lambda: _format_jobs(jobs))
    return 0


def _cmd_dlq(args: argparse.Namespace) -> int:
    with _open(args) as q:
        if args.requeue is not None:
            job_id = None if args.requeue == "__all__" else args.requeue
            count = q.requeue_dead(job_id)
            _emit(args, {"requeued": count}, lambda: f"requeued {count} job(s)")
            return 0
        jobs = q.dlq()
    _emit(args, [j.to_dict() for j in jobs], lambda: _format_jobs(jobs) or "DLQ is empty")
    return 0


def _cmd_purge(args: argparse.Namespace) -> int:
    if not args.yes:
        # Destructive and irreversible, so it asks. --yes is there for scripts.
        reply = input(f"delete every job in {args.dir}/{args.queue}? [y/N] ")
        if reply.strip().lower() not in {"y", "yes"}:
            print("aborted")
            return 1
    with _open(args) as q:
        count = q.purge()
    _emit(args, {"purged": count}, lambda: f"purged {count} job(s)")
    return 0


def _cmd_compact(args: argparse.Namespace) -> int:
    with _open(args) as q:
        result = q.compact()
    _emit(
        args,
        {
            "bytes_before": result.bytes_before,
            "bytes_after": result.bytes_after,
            "jobs_kept": result.jobs_kept,
            "jobs_dropped": result.jobs_dropped,
        },
        result.describe,
    )
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    """Print the log record by record -- the durability story, made checkable."""
    with _open(args) as q:
        records = list(q._log.iter_all())
    if args.limit:
        records = records[: args.limit]
    rows = [
        {"offset": r.offset, "type": r.type.name, "payload": r.payload} for r in records
    ]
    _emit(
        args,
        rows,
        lambda: "\n".join(
            f"{r['offset']:>10}  {r['type']:<8}  {json.dumps(r['payload'], sort_keys=True)}"
            for r in rows
        ),
    )
    return 0


def _cmd_recover(args: argparse.Namespace) -> int:
    with _open(args) as q:
        report = q.recovery
        stats = q.stats()
    data = {
        "records_applied": report.records_applied,
        "bytes_scanned": report.bytes_scanned,
        "bytes_discarded": report.bytes_discarded,
        "damage": report.damage.value,
        "repaired": report.repaired,
        "orphan_records": report.orphan_records,
    } | stats.to_dict()
    _emit(args, data, lambda: f"{report.describe()}\n{stats.to_dict()}")
    return 0


def _cmd_work(args: argparse.Namespace) -> int:
    from .worker import Worker

    handler = _load_handler(args.handler)
    with _open(args) as q:
        worker = Worker(
            q,
            handler,
            concurrency=args.concurrency,
            max_jobs=args.max_jobs,
            idle_timeout=args.idle_timeout,
            poll_interval=args.poll_interval,
        )
        stats = worker.run()
    _emit(
        args,
        stats.to_dict(),
        lambda: " ".join(f"{k}={v}" for k, v in stats.to_dict().items()),
    )
    return 0


def _cmd_bench(args: argparse.Namespace) -> int:
    from .bench import run_benchmarks, format_markdown, format_plain

    results = run_benchmarks(jobs=args.jobs)
    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    elif args.markdown:
        print(format_markdown(results))
    else:
        print(format_plain(results))
    return 0


# -- helpers --------------------------------------------------------------


def _parse_payload(text: str) -> Any:
    """Interpret a CLI payload as JSON, falling back to the literal string.

    ``nobroker enqueue hello`` should work. So should ``enqueue '{"a":1}'``.
    Guessing is friendlier than making every shell user quote a JSON string.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _load_handler(spec: str) -> Callable[[Job], Any]:
    """Resolve ``package.module:callable`` to the callable itself."""
    if ":" not in spec:
        raise NobrokerError(
            f"handler {spec!r} must look like 'package.module:function'"
        )
    module_name, _, attr = spec.partition(":")
    sys.path.insert(0, "")  # make the current directory importable, like pytest does
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise NobrokerError(f"could not import {module_name!r}: {exc}") from exc
    try:
        handler = getattr(module, attr)
    except AttributeError as exc:
        raise NobrokerError(f"{module_name!r} has no attribute {attr!r}") from exc
    if not callable(handler):
        raise NobrokerError(f"{spec} is not callable")
    return handler


def _format_jobs(jobs: Sequence[Job]) -> str:
    if not jobs:
        return ""
    lines = [f"{'ID':<34}{'STATE':<9}{'PRI':>4}{'ATT':>5}  PAYLOAD"]
    for job in jobs:
        payload = json.dumps(job.payload, sort_keys=True)
        if len(payload) > 60:
            payload = payload[:57] + "..."
        lines.append(
            f"{job.id:<34}{job.state.value:<9}{job.priority:>4}"
            f"{job.attempts:>3}/{job.max_attempts:<2} {payload}"
        )
    return "\n".join(lines)


def _emit(args: argparse.Namespace, data: Any, plain: Callable[[], str]) -> None:
    """Print JSON or human text. ``plain`` is a callable so it is never
    formatted when the caller asked for JSON."""
    if args.json:
        print(json.dumps(data, indent=2, sort_keys=True, default=str))
    else:
        text = plain()
        if text:
            print(text)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

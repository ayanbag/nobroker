#!/usr/bin/env python3
"""One command for everything -- without needing ``make`` installed.

    python make.py build      # -> dist/nobroker.pyz, one runnable file
    python make.py demo       # the end-to-end tour
    python make.py test       # 122 tests, including crash recovery
    python make.py            # list every task

Why this exists
---------------
There is a ``Makefile`` too, and it delegates to this file so the two can never
drift. But ``make`` is a **separately installed tool**, which makes it a
dependency of the build even though it never appears in a manifest -- and it is
genuinely absent on a stock Windows box, where `make build` fails before it can
print anything.

The project's claim is "you need Python and nothing else." A task runner written
in the standard library keeps that claim true of the build itself, not just of
the library. Every task below is one ``subprocess`` call to ``sys.executable`` --
the interpreter already running this file, not an external program.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

# Every task: name -> (argv after the interpreter, one-line help).
# Keeping these as argv lists rather than shell strings means no quoting rules,
# no shell, and identical behaviour on every platform.
TASKS: dict[str, tuple[list[str], str]] = {
    "build": (
        ["tools/build_pyz.py"],
        "Bundle the tool into one runnable file (dist/nobroker.pyz)",
    ),
    "demo": (
        ["examples/demo.py"],
        "Run the end-to-end tour (start here)",
    ),
    "video-demo": (
        ["examples/video_demo.py"],
        "Run the paced, narrated demo built for screen recording",
    ),
    "test": (
        ["-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        "Run the full test suite, including crash recovery",
    ),
    "test-quick": (
        ["-m", "unittest", "discover", "-s", "tests", "-t", "."],
        "Run everything except the slow exhaustive-truncation sweep",
    ),
    "bench": (
        ["-m", "nobroker.cli", "bench", "--jobs", "5000"],
        "Measure throughput on this machine",
    ),
    "bench-md": (
        ["-m", "nobroker.cli", "bench", "--jobs", "5000", "--markdown"],
        "Same, as a Markdown table for the README",
    ),
    "check-deps": (
        ["tools/check_deps.py"],
        "Fail if anything outside the standard library is imported",
    ),
    "cli": (
        ["-m", "nobroker.cli", "--help"],
        "Show the CLI help",
    ),
}

# Tasks that need an environment variable set, beyond PYTHONPATH.
TASK_ENV: dict[str, dict[str, str]] = {
    "test-quick": {"NOBROKER_SKIP_SLOW": "1"},
}


def _env() -> dict[str, str]:
    """Child environment with ``src`` importable.

    Prepended rather than replaced, so a caller who has already put something on
    PYTHONPATH does not silently lose it.
    """
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC}{os.pathsep}{existing}" if existing else str(SRC)
    return env


def run(task: str, extra: list[str] | None = None) -> int:
    """Run one task, returning its exit code.

    Extra arguments are forwarded to the task, so ``python make.py video-demo
    --scenes 6`` and ``python make.py bench --jobs 100`` both work without a
    wrapper flag per option.
    """
    argv, _ = TASKS[task]
    env = _env() | TASK_ENV.get(task, {})
    started = time.perf_counter()
    completed = subprocess.run([sys.executable, *argv, *(extra or [])], cwd=ROOT, env=env)
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        print(f"\n{task} FAILED (exit {completed.returncode}) after {elapsed:.1f}s")
    return completed.returncode


def deps_proof() -> int:
    """Regenerate deps-proof.txt from this machine and interpreter."""
    target = ROOT / "deps-proof.txt"
    completed = subprocess.run(
        [sys.executable, "tools/check_deps.py"],
        cwd=ROOT,
        env=_env(),
        capture_output=True,
        text=True,
    )
    # Write regardless of exit code: a proof that records a third-party import is
    # still the truthful proof, and silently keeping the old clean one would be
    # the dishonest option.
    target.write_text(completed.stdout, encoding="utf-8")
    print(completed.stdout, end="")
    print(f"wrote {target.relative_to(ROOT).as_posix()}")
    return completed.returncode


def clean() -> int:
    """Remove caches, scratch queues and build output."""
    removed = 0
    for path in [ROOT / "dist", ROOT / ".nobroker", ROOT / "build"]:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
    for cache in ROOT.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
        removed += 1
    print(f"cleaned ({removed} paths)")
    return 0


def usage() -> int:
    print("nobroker -- a durable, broker-less job queue with zero dependencies")
    print()
    width = max(len(name) for name in [*TASKS, "deps-proof", "clean"])
    for name, (_, help_text) in TASKS.items():
        print(f"  {name:<{width}}  {help_text}")
    print(f"  {'deps-proof':<{width}}  Regenerate deps-proof.txt")
    print(f"  {'clean':<{width}}  Remove caches, scratch queues and build output")
    print()
    print(f"  usage: python make.py <task>      ({sys.executable})")
    print(f"  python: {sys.version.split()[0]} on {sys.platform}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("task", nargs="?")
    args, extra = parser.parse_known_args()

    if args.task in (None, "help", "-h", "--help"):
        return usage()
    if args.task in ("deps-proof", "clean"):
        if extra:
            print(f"error: {args.task} takes no arguments", file=sys.stderr)
            return 2
        return deps_proof() if args.task == "deps-proof" else clean()
    if args.task not in TASKS:
        print(f"error: no task {args.task!r}\n", file=sys.stderr)
        usage()
        return 2
    return run(args.task, extra)


if __name__ == "__main__":
    raise SystemExit(main())

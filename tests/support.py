"""Shared test helpers.

The interesting one is :func:`run_child_and_kill`, which is how the crash tests
kill a process without shelling out to ``kill``. Invoking an external ``kill``
binary would be a dependency on a tool that is not the Python standard library --
the exact thing this project is not allowed to have -- and it would not work on
Windows either. ``os._exit(9)`` from inside the child is the honest equivalent:
it terminates immediately, skipping ``atexit`` handlers, ``finally`` blocks,
buffer flushes and destructors. Whatever is on disk at that instant is all the
recovery code will ever get.
"""

from __future__ import annotations

import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable


class TempQueueTest(unittest.TestCase):
    """Base class providing a scratch directory per test."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="nobroker-test-")
        self.addCleanup(self._cleanup)
        self.dir = Path(self._tmp.name)

    def _cleanup(self) -> None:
        # Windows refuses to delete a directory whose files are still open by a
        # process that has not exited; ignore_cleanup_errors keeps a stray handle
        # from failing an otherwise passing test.
        try:
            self._tmp.cleanup()
        except OSError:
            pass


def run_child_and_kill(target: Callable[..., None], *args: Any) -> int:
    """Run ``target`` in a child process that ends with ``os._exit(9)``.

    Returns the child's raw exit status. ``target`` is responsible for calling
    ``os._exit(9)`` itself -- that is what makes this a *crash* rather than a
    return.

    Uses :func:`os.fork` where it exists, as the plainest possible expression of
    "same code, new process, no cleanup". Windows has no fork, so there the same
    child function is launched through :mod:`multiprocessing` with the ``spawn``
    start method; ``os._exit(9)`` inside it is just as abrupt.
    """
    if hasattr(os, "fork"):
        pid = os.fork()
        if pid == 0:  # pragma: no cover - runs only in the child
            try:
                target(*args)
            finally:
                os._exit(9)
        _pid, status = os.waitpid(pid, 0)
        return status

    ctx = multiprocessing.get_context("spawn")
    process = ctx.Process(target=target, args=args)
    process.start()
    process.join(timeout=60)
    if process.is_alive():  # pragma: no cover - only on a hung child
        process.kill()
        process.join()
        raise AssertionError("crash-test child did not exit")
    return process.exitcode or 0

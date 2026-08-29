"""Exception hierarchy for nobroker.

Every failure mode gets a named type. Callers catch :class:`NobrokerError` to
mean "the queue itself went wrong" without swallowing bugs in their own handler
code, which is what a bare ``except`` would do.
"""

from __future__ import annotations


class NobrokerError(Exception):
    """Base class for every error raised by nobroker."""


class QueueClosedError(NobrokerError):
    """Raised when an operation is attempted on a closed queue."""


class LockTimeoutError(NobrokerError):
    """Raised when the cross-process queue lock could not be acquired in time.

    A held lock means another process is mid-operation. Operations are short, so
    a timeout usually means a process was suspended (SIGSTOP, debugger) rather
    than that the queue is busy.
    """


class CorruptLogError(NobrokerError):
    """Raised when the log is damaged in a way recovery cannot resolve.

    A torn *tail* is expected after a crash and is repaired silently by
    truncation. This is for damage that is not a torn tail: a bad file header,
    an unknown format version, or a record type the reader does not understand.
    """


class JobNotFoundError(NobrokerError):
    """Raised when a job id is not present in the queue."""


class NotLeasedError(NobrokerError):
    """Raised when acking/nacking a job that is not currently leased.

    This is the visible symptom of a lost lease: the visibility timeout expired,
    another worker picked the job up, and the original worker finally finished.
    At-least-once delivery makes this a normal event, not a bug -- it is exactly
    why handlers must be idempotent.
    """


class CompactionError(NobrokerError):
    """Raised when compaction could not complete and the live log is untouched.

    Compaction is always all-or-nothing: on failure the original log remains the
    authoritative one, so the queue is never left in a half-compacted state.
    """


class SerializationError(NobrokerError):
    """Raised when a payload cannot be encoded to, or decoded from, JSON."""

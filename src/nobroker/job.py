"""The job record and its state machine.

A :class:`Job` is a plain dataclass. Everything the queue needs to make a
decision about a job lives on the job itself, so replaying the log is just
rebuilding these objects -- there is no second source of truth.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field, replace
from typing import Any


class JobState(str, enum.Enum):
    """Where a job is in its life cycle.

    Inherits from ``str`` so a state serialises to JSON as its own name; the log
    stays readable with ``cat`` and a decoder, which matters when you are
    debugging a durability bug at 3am.

    ``DELAYED`` is deliberately *not* a state. A job that is not yet eligible is
    ``READY`` with ``available_at`` in the future. One less state to keep
    consistent, and eligibility stays a pure function of the clock.
    """

    READY = "ready"
    LEASED = "leased"
    DONE = "done"
    DEAD = "dead"


@dataclass(slots=True)
class Job:
    """A unit of work and everything known about its delivery history.

    Attributes:
        id: Opaque unique identifier, assigned at enqueue.
        payload: Any JSON-serialisable value. nobroker never inspects it.
        priority: Higher leases first. Ties break by ``available_at`` then FIFO.
        state: See :class:`JobState`.
        attempts: Number of times this job has been *leased*, not completed.
        max_attempts: Attempts allowed before the job is moved to the DLQ.
        available_at: Wall-clock time before which the job may not be leased.
        enqueued_at: Wall-clock time the job was first accepted.
        lease_deadline: When the current lease expires; ``None`` unless LEASED.
        lease_token: Identifies *which* lease is current. A worker holds the
            token it was handed; if its lease expires and the job is redelivered
            elsewhere, the token changes and the late worker's ack is rejected
            instead of silently completing somebody else's delivery. This is the
            fencing token that makes at-least-once safe to reason about.
        last_error: Error text from the most recent nack, for post-mortems.
        seq: Monotonic per-log insertion counter, used to make ordering total.
        version: Bumped on every state change. Heap entries carry the version
            they were pushed with, which is how stale entries are recognised and
            discarded lazily instead of being searched for and removed.
    """

    id: str
    payload: Any
    priority: int = 0
    state: JobState = JobState.READY
    attempts: int = 0
    max_attempts: int = 5
    available_at: float = 0.0
    enqueued_at: float = 0.0
    lease_deadline: float | None = None
    lease_token: str | None = None
    last_error: str | None = None
    seq: int = 0
    version: int = 0
    _extra: dict[str, Any] = field(default_factory=dict, repr=False)

    def snapshot(self) -> "Job":
        """Return a detached copy safe to hand to caller code.

        The queue's in-memory index owns the live instances. Handing those out
        would let a handler mutate queue state by assigning an attribute, so
        every public API returns a copy instead.
        """
        return replace(self, _extra=dict(self._extra))

    def is_eligible(self, now: float) -> bool:
        """True if this job may be leased right now."""
        return self.state is JobState.READY and self.available_at <= now

    def to_dict(self) -> dict[str, Any]:
        """Serialise for the log and for ``--json`` CLI output."""
        return {
            "id": self.id,
            "payload": self.payload,
            "priority": self.priority,
            "state": self.state.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "available_at": self.available_at,
            "enqueued_at": self.enqueued_at,
            "lease_deadline": self.lease_deadline,
            "lease_token": self.lease_token,
            "last_error": self.last_error,
            "seq": self.seq,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        """Rebuild a job from its serialised form.

        Unknown keys are kept in ``_extra`` rather than dropped, so a log written
        by a newer nobroker survives a round trip through an older one.
        """
        known = {
            "id",
            "payload",
            "priority",
            "state",
            "attempts",
            "max_attempts",
            "available_at",
            "enqueued_at",
            "lease_deadline",
            "lease_token",
            "last_error",
            "seq",
        }
        return cls(
            id=data["id"],
            payload=data.get("payload"),
            priority=int(data.get("priority", 0)),
            state=JobState(data.get("state", "ready")),
            attempts=int(data.get("attempts", 0)),
            max_attempts=int(data.get("max_attempts", 5)),
            available_at=float(data.get("available_at", 0.0)),
            enqueued_at=float(data.get("enqueued_at", 0.0)),
            lease_deadline=data.get("lease_deadline"),
            lease_token=data.get("lease_token"),
            last_error=data.get("last_error"),
            seq=int(data.get("seq", 0)),
            _extra={k: v for k, v in data.items() if k not in known},
        )

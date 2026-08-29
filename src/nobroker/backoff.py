"""Retry delay policy: exponential growth with jitter.

Jitter is not decoration. Without it, a downstream outage that fails 10k jobs at
once produces 10k retries at the same instant, then again at the same instant,
forever -- the queue turns into a synchronised hammer aimed at the thing that
was already unhealthy. Spreading the retries is the entire point.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Computes how long a failed job waits before it may be leased again.

    Attributes:
        base: Delay in seconds after the first failure.
        factor: Multiplier applied per additional attempt.
        max_delay: Ceiling, so attempt 30 does not schedule a retry in 2089.
        jitter: Fraction of the computed delay that is randomised, in ``[0, 1]``.
            ``0.0`` is deterministic (used by tests), ``1.0`` is AWS "full
            jitter". The default of ``0.5`` is "equal jitter": half the delay is
            fixed, half is random, which keeps retries spread out without
            letting a job retry almost immediately.
    """

    base: float = 1.0
    factor: float = 2.0
    max_delay: float = 300.0
    jitter: float = 0.5

    def __post_init__(self) -> None:
        if self.base < 0:
            raise ValueError("base must be >= 0")
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter must be in [0, 1]")

    def delay_for(self, attempts: int, rng: random.Random | None = None) -> float:
        """Seconds to wait before retrying a job that has failed ``attempts`` times.

        The result is *sampled here*, at nack time, and then written into the log
        as an absolute timestamp. Replay therefore never re-rolls the dice: a log
        replayed twice produces byte-identical state. Non-determinism is resolved
        once, at write time, on purpose.
        """
        if attempts <= 0:
            return 0.0
        raw = min(self.base * (self.factor ** (attempts - 1)), self.max_delay)
        if self.jitter == 0.0:
            return raw
        fixed = raw * (1.0 - self.jitter)
        rand = (rng or random).uniform(0.0, raw * self.jitter)
        return fixed + rand

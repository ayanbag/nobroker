"""Retry delay policy."""

from __future__ import annotations

import random
import unittest

from nobroker import BackoffPolicy


class BackoffTest(unittest.TestCase):
    def test_delay_grows_exponentially(self) -> None:
        policy = BackoffPolicy(base=1.0, factor=2.0, jitter=0.0, max_delay=1e9)
        self.assertEqual(
            [policy.delay_for(n) for n in range(1, 6)], [1.0, 2.0, 4.0, 8.0, 16.0]
        )

    def test_zero_attempts_means_no_delay(self) -> None:
        self.assertEqual(BackoffPolicy().delay_for(0), 0.0)

    def test_max_delay_is_a_hard_ceiling(self) -> None:
        policy = BackoffPolicy(base=1.0, factor=2.0, jitter=0.0, max_delay=10.0)
        # Attempt 40 would otherwise schedule a retry some centuries out.
        self.assertEqual(policy.delay_for(40), 10.0)

    def test_jitter_stays_within_its_band(self) -> None:
        policy = BackoffPolicy(base=8.0, factor=1.0, jitter=0.5, max_delay=1e9)
        samples = [policy.delay_for(1) for _ in range(500)]
        # Equal jitter: half fixed, half random. Never early, never more than
        # the nominal delay.
        self.assertTrue(all(4.0 <= s <= 8.0 for s in samples))
        self.assertGreater(len(set(samples)), 100, "jitter should actually vary")

    def test_full_jitter_can_retry_almost_immediately(self) -> None:
        policy = BackoffPolicy(base=8.0, factor=1.0, jitter=1.0, max_delay=1e9)
        samples = [policy.delay_for(1) for _ in range(500)]
        self.assertTrue(all(0.0 <= s <= 8.0 for s in samples))
        self.assertLess(min(samples), 1.0)

    def test_jitter_is_reproducible_with_a_seeded_rng(self) -> None:
        policy = BackoffPolicy(base=4.0, jitter=0.5)
        a = policy.delay_for(3, rng=random.Random(1234))
        b = policy.delay_for(3, rng=random.Random(1234))
        self.assertEqual(a, b)

    def test_invalid_settings_are_rejected_at_construction(self) -> None:
        for kwargs in ({"base": -1}, {"factor": 0.5}, {"jitter": 1.5}, {"jitter": -0.1}):
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError):
                    BackoffPolicy(**kwargs)

    def test_policy_is_immutable(self) -> None:
        """Frozen so a policy shared between queues cannot be edited by one."""
        policy = BackoffPolicy()
        with self.assertRaises(Exception):
            policy.base = 99.0  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()

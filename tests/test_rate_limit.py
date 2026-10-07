"""Rolling request/token admission without wall-clock waits or network access."""

import unittest

from dowser.rate_limit import RateLimiter, shared_rate_limiter


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def read(self):
        return self.now

    async def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay


class RateTests(unittest.IsolatedAsyncioTestCase):
    async def test_requests_are_bounded_in_a_rolling_second(self):
        clock = Clock()
        limiter = RateLimiter(2, 100, clock=clock.read, sleeper=clock.sleep)
        await limiter.acquire(1, 10)
        await limiter.acquire(1, 10)
        third = await limiter.acquire(1, 10)
        self.assertEqual(third.started, 1)
        self.assertEqual(third.waited, 1)
        self.assertEqual(clock.waits, [1])

    async def test_unknown_tokens_reserve_capacity_until_window_expires(self):
        clock = Clock()
        limiter = RateLimiter(80, 10, clock=clock.read, sleeper=clock.sleep)
        await limiter.acquire(8, 10)
        second = await limiter.acquire(8, 10)
        self.assertEqual(second.started, 1)
        self.assertEqual(clock.waits, [1])

    async def test_known_usage_reconciles_capacity_and_unsent_admissions_are_discarded(
        self,
    ):
        clock = Clock()
        limiter = RateLimiter(80, 10, clock=clock.read, sleeper=clock.sleep)
        first = await limiter.acquire(8, 10)
        limiter.reconcile(first, 2)
        second = await limiter.acquire(8, 10)
        self.assertEqual(second.started, 0)
        limiter.discard(second)
        third = await limiter.acquire(8, 10)
        self.assertEqual(third.started, 0)
        self.assertEqual(clock.waits, [])

    async def test_admission_honors_provider_deadline_and_rejects_impossible_request(
        self,
    ):
        clock = Clock()
        limiter = RateLimiter(1, 10, clock=clock.read, sleeper=clock.sleep)
        await limiter.acquire(8, 10)
        with self.assertRaises(TimeoutError):
            await limiter.acquire(1, 0.5)
        with self.assertRaises(ValueError):
            await limiter.acquire(11, 10)
        self.assertEqual(clock.waits, [])

    async def test_shared_campaign_window_survives_per_trial_provider_creation(self):
        first = shared_rate_limiter("rate-test", 80, 100000)
        self.assertIs(first, shared_rate_limiter("rate-test", 80, 100000))
        with self.assertRaises(ValueError):
            shared_rate_limiter("rate-test", 40, 100000)


if __name__ == "__main__":
    unittest.main()

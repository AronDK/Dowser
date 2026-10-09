"""Thread-safe rolling admission shared across provider worker loops."""

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass(eq=False)
class Admission:
    started: float
    tokens: int
    waited: float


class RateLimiter:
    def __init__(
        self,
        requests_per_second,
        tokens_per_second,
        *,
        clock=time.monotonic,
        sleeper=asyncio.sleep,
        window_seconds=1,
    ):
        if requests_per_second < 1 or tokens_per_second < 1:
            raise ValueError("rate limits must be positive")
        self.requests_per_second = requests_per_second
        self.tokens_per_second = tokens_per_second
        self.clock, self.sleeper = clock, sleeper
        self.window_seconds = window_seconds
        self.lock = threading.Lock()
        self.window = deque()

    async def acquire(self, tokens, deadline=None):
        if type(tokens) is not int or not 0 <= tokens <= self.tokens_per_second:
            raise ValueError("request exceeds token rate allowance")
        started = self.clock()
        while True:
            with self.lock:
                current = self.clock()
                if deadline is not None and current >= deadline:
                    raise TimeoutError("rate admission exceeded provider deadline")
                while (
                    self.window
                    and self.window[0].started <= current - self.window_seconds
                ):
                    self.window.popleft()
                if (
                    len(self.window) < self.requests_per_second
                    and sum(a.tokens for a in self.window) + tokens
                    <= self.tokens_per_second
                ):
                    ticket = Admission(current, tokens, current - started)
                    self.window.append(ticket)
                    return ticket
                delay = max(
                    0.001, self.window[0].started + self.window_seconds - current
                )
                if deadline is not None and current + delay >= deadline:
                    raise TimeoutError("rate admission exceeded provider deadline")
            await self.sleeper(delay)

    def reconcile(self, ticket, tokens):
        if type(tokens) is not int or tokens < 0:
            raise ValueError("invalid actual token usage")
        with self.lock:
            ticket.tokens = tokens

    def discard(self, ticket):
        with self.lock:
            if ticket in self.window:
                self.window.remove(ticket)


_shared = {}
_shared_lock = threading.Lock()


def shared_rate_limiter(
    key, requests_per_second, tokens_per_second, *, window_seconds=1
):
    """Keep one campaign admission window across per-trial provider construction."""
    with _shared_lock:
        if key not in _shared:
            _shared[key] = RateLimiter(
                requests_per_second, tokens_per_second, window_seconds=window_seconds
            )
        limiter = _shared[key]
        if (
            limiter.requests_per_second,
            limiter.tokens_per_second,
            limiter.window_seconds,
        ) != (
            requests_per_second,
            tokens_per_second,
            window_seconds,
        ):
            raise ValueError("shared rate policy changed")
        return limiter

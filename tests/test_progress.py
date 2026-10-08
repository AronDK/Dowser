"""Offline regressions for investigative progress and provider deadlines."""

import asyncio
import io
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dowser.contracts import AppContext
from dowser.models import ActionCandidate, DecisionRequest, IncidentState, Limits
from dowser.rate_limit import RateLimiter
from dowser.runtime import bounded_call
from plugins.itbench_aa import ProviderSettings
from plugins.jev import JevError, JevProvider, JevSettings


class TimeoutTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.request = DecisionRequest(
            incident_id="timing",
            state=IncidentState(
                incident_id="timing",
                alert={},
                desired_state={},
                resources=[
                    {"id": "one", "platform": "fixture", "platform_version": "1"}
                ],
            ),
            candidates=[
                ActionCandidate(
                    id="read",
                    tool="fixture.read",
                    plugin_version="1",
                    args={},
                    description="Read",
                    resources=["one"],
                    verification="check",
                    effect="read_only",
                )
            ],
        )
        self.context = AppContext({}, Limits(), Path.cwd())
        self.response = {
            "model": "jev-1.13.0",
            "usage": {"input_tokens": 10, "output_tokens": 1},
            "answers": {
                "next_action": {
                    "type": "choice",
                    "choice": "c0",
                    "confidence": 1.0,
                    "probabilities": {"c0": 1.0, "wait": 0.0, "escalate": 0.0},
                }
            },
        }

    async def invoke(self, handle, settings, call=None):
        provider = JevProvider(settings, self.context)
        original = httpx.AsyncClient
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-credential"}),
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kw: original(
                    **kw, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("sys.stderr", io.StringIO()),
        ):
            return await (call(provider) if call else provider.decide(self.request))

    def test_alias_defaults_and_conflicts(self):
        self.assertEqual(JevSettings().decision_timeout_seconds, 30)
        self.assertIsNone(
            ProviderSettings(ledger="unused", trial="test").decision_timeout_seconds
        )
        self.assertEqual(JevSettings(timeout_seconds=12.0).decision_timeout_seconds, 12)
        with self.assertRaises(ValueError):
            JevSettings(timeout_seconds=10.0, decision_timeout_seconds=None)

    async def test_delay_beyond_former_wrapper_survives(self):
        # A real >10s response proves removal of the old bound.
        async def handle(request):
            await asyncio.sleep(10.05)
            return httpx.Response(200, json=self.response)

        result = await self.invoke(handle, JevSettings(decision_timeout_seconds=None))
        self.assertEqual(result.operation, "select")
        self.assertIsNone(result.score_metadata["timing_spans"][0]["server"])

    async def test_attempt_timeout_retries_without_overlap(self):
        active = 0
        calls = 0

        async def handle(request):
            nonlocal active, calls
            active += 1
            calls += 1
            self.assertEqual(active, 1)
            try:
                if calls == 1:
                    await asyncio.sleep(1)
                return httpx.Response(200, json=self.response)
            finally:
                active -= 1

        result = await self.invoke(
            handle,
            JevSettings(
                decision_timeout_seconds=None,
                attempt_timeout_seconds=0.02,
                retry_initial_seconds=0.001,
            ),
        )
        self.assertEqual(calls, 2)
        self.assertEqual(
            result.score_metadata["retry_failures"][0]["timeout_kind"], "attempt"
        )

    async def test_retry_after_without_aggregate_and_cancellation(self):
        calls = 0

        async def handle(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, headers={"retry-after": "0.03"})
            return httpx.Response(200, json=self.response)

        result = await self.invoke(
            handle,
            JevSettings(decision_timeout_seconds=None, retry_initial_seconds=0.001),
        )
        self.assertEqual(calls, 2)
        self.assertGreaterEqual(
            result.score_metadata["timing_spans"][0]["backoff"], 0.03
        )

    async def test_aggregate_and_incident_and_stop_are_distinct(self):
        async def handle(request):
            await asyncio.sleep(10)

        with self.assertRaises(JevError) as aggregate:
            await self.invoke(handle, JevSettings(decision_timeout_seconds=0.02))
        self.assertEqual(aggregate.exception.detail.timeout_kind, "aggregate")

        async def incident(provider):
            return await bounded_call(provider.decide, self.request, seconds=0.02)

        with self.assertRaises((JevError, TimeoutError)):
            await self.invoke(
                handle, JevSettings(decision_timeout_seconds=None), incident
            )

        async def stop(provider):
            task = asyncio.create_task(provider.decide(self.request))
            await asyncio.sleep(0.02)
            task.cancel()
            return await task

        with self.assertRaises(asyncio.CancelledError):
            await self.invoke(handle, JevSettings(decision_timeout_seconds=None), stop)

    async def test_admission_without_deadline_retains_unknown_reservations(self):
        limiter = RateLimiter(80, 100000)
        admission = await limiter.acquire(66048, None)
        self.assertEqual(admission.tokens, 66048)
        self.assertEqual(limiter.window[0].tokens, 66048)
        limiter.reconcile(admission, 11)
        self.assertEqual(limiter.window[0].tokens, 11)

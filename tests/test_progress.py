"""Offline regressions for investigative progress and provider deadlines."""

import asyncio
import io
import json
import os
import tempfile
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


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        import csv

        from dowser.bench_data import EvidenceIndex, create_index

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.node = "cluster/Node/worker"
        self.pod = "demo/Pod/shipping"
        objects = [
            {
                "kind": "Node",
                "metadata": {
                    "name": "worker",
                    "resourceVersion": "1",
                    "managedFields": [{"noise": "x" * 10000}],
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            },
            {
                "kind": "Pod",
                "metadata": {"name": "shipping", "namespace": "demo"},
                "spec": {
                    "nodeName": "worker",
                    "containers": [
                        {
                            "image": "shipping:1",
                            "env": [{"name": "QUOTE_ADDR", "value": "quote:0000"}],
                            "resources": {"limits": {"cpu": "1"}},
                        }
                    ],
                },
            },
            {
                "kind": "Node",
                "metadata": {
                    "name": "worker",
                    "resourceVersion": "2",
                    "managedFields": [{"manager": "new"}],
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            },
            {
                "kind": "ConfigMap",
                "metadata": {"name": "flagd", "namespace": "demo"},
                "data": {"flags.json": '{"paymentFailure":true}'},
            },
        ]
        with (root / "k8s_objects_raw.tsv").open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Timestamp", "Body", "ResourceAttributes"],
                delimiter="\t",
            )
            writer.writeheader()
            for i, obj in enumerate(objects):
                writer.writerow(
                    {
                        "Timestamp": str(i),
                        "Body": json.dumps(obj),
                        "ResourceAttributes": json.dumps({"k8s.node.name": "worker"}),
                    }
                )
        with (root / "k8s_events_raw.tsv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["Body"], delimiter="\t")
            writer.writeheader()
            writer.writerow(
                {
                    "Body": json.dumps(
                        {
                            "kind": "Event",
                            "metadata": {"name": "failed", "namespace": "demo"},
                            "regarding": {
                                "kind": "Pod",
                                "namespace": "demo",
                                "name": "shipping",
                            },
                            "reason": "Failed",
                            "note": "connection refused",
                        }
                    )
                }
            )
        path = root / "index.sqlite3"
        create_index(root, path, 1)
        self.index = EvidenceIndex(path, 1)

    def test_owners_and_related_records_are_separate(self):
        self.assertEqual(len(self.index.refs(self.node, "configuration")), 2)
        pod_record = self.index.refs(self.pod, "configuration")[0]
        self.assertFalse(self.index.owns(self.node, pod_record))
        self.assertIn(
            pod_record, self.index.refs(self.node, "configuration", related=True)
        )
        related = self.index.page(self.node, "related_configuration")
        self.assertEqual(related["records"][0]["ownership"], "related")

    def test_projection_collapses_churn_and_retains_config_and_events(self):
        history = self.index.page(self.node, "history")
        self.assertEqual(history["records"][0]["collapsed_revisions"], 2)
        self.assertEqual(history["source_records"], 2)
        self.assertIsNone(history["next"])
        self.assertNotIn("managedFields", history["records"][0]["content"])
        self.assertIn('"Ready"', history["records"][0]["content"])
        self.assertIn(
            "quote:0000",
            self.index.page(self.pod, "configuration")["records"][0]["content"],
        )
        cfg = self.index.page("demo/ConfigMap/flagd", "configuration")
        self.assertIn('"paymentFailure":true', cfg["records"][0]["content"])
        event = self.index.page(self.pod, "events")
        self.assertIn("connection refused", event["records"][0]["content"])
        self.assertIn('"regarding"', event["records"][0]["content"])
        self.assertTrue(self.index.owns(self.pod, event["records"][0]["id"]))
        raw = self.index.page(self.node, "raw_configuration", offset=1)
        self.assertIn("managedFields", raw["records"][0]["content"])
        self.assertTrue(
            self.index.page("demo/Pod/missing", "history")["missing_evidence"]
        )

    def test_existing_indexes_are_never_overwritten(self):
        from dowser.bench_data import create_index

        before = self.index.path.read_bytes()
        with self.assertRaisesRegex(ValueError, "separate root"):
            create_index(self.index.path.parent, self.index.path, 1)
        self.assertEqual(before, self.index.path.read_bytes())

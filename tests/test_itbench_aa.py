"""Offline benchmark boundary, scoring, spending, and full campaign tests."""

import asyncio
import csv
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import yaml

from dowser.bench import DEFAULTS, failure_category, report, run_campaign, run_trial
from dowser.bench_data import (
    PILOT,
    REVISION,
    EvidenceIndex,
    atomic_json,
    confined,
    create_index,
    dumps,
    file_hash,
    fingerprint,
)
from dowser.bench_ledger import CallLimit, Ledger, SpendingLimit
from dowser.bench_score import score
from dowser.contracts import AppContext
from dowser.models import DecisionRequest, Limits, RawIncident
from plugins.itbench_aa import (
    BudgetedProvider,
    Normalizer,
    Plugin,
    ProviderSettings,
    Settings,
    bounded_state,
)
from plugins.jev import JevError

CANARY = "MUST_NEVER_ENTER_MODEL_927"
ENTITY = "otel-demo/ConfigMap/flagd-config"


def fixture(root, scenario=8, *, large=False):
    root.mkdir(parents=True, exist_ok=True)
    objects = [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "flagd-config",
                "namespace": "otel-demo",
                "resourceVersion": "1",
            },
            "data": {
                "flags": '{"paymentFailure":true}',
                "api_key": CANARY,
                "long": "é\n" * 5000 if large else "config",
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "payment-abc",
                "namespace": "otel-demo",
                "labels": {"app": "payment"},
                "ownerReferences": [{"kind": "ReplicaSet", "name": "payment-rs"}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "payment-rs",
                "namespace": "otel-demo",
                "ownerReferences": [{"kind": "Deployment", "name": "payment"}],
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "payment", "namespace": "otel-demo"},
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "payment", "namespace": "otel-demo"},
            "spec": {"selector": {"app": "payment"}},
        },
        {"apiVersion": "v1", "kind": "Node", "metadata": {"name": "worker"}},
    ]
    objects.append(
        {
            **objects[0],
            "metadata": {**objects[0]["metadata"], "resourceVersion": "2"},
            "recommended_actions": CANARY,
        }
    )
    with (root / "k8s_objects_raw.tsv").open("w", newline="") as h:
        writer = csv.DictWriter(
            h, fieldnames=["Timestamp", "Body", "ResourceAttributes"], delimiter="\t"
        )
        writer.writeheader()
        for i, obj in enumerate(objects):
            writer.writerow(
                {
                    "Timestamp": f"2025-01-01T00:00:{i:02d}Z",
                    "Body": json.dumps(obj),
                    "ResourceAttributes": "{'k8s.namespace.name': 'otel-demo'}",
                }
            )
    with (root / "otel_logs_raw.tsv").open("w", newline="") as h:
        writer = csv.DictWriter(
            h, fieldnames=["Timestamp", "Body", "ResourceAttributes"], delimiter="\t"
        )
        writer.writeheader()
        writer.writerow(
            {
                "Timestamp": "2025-01-01T00:00:00Z",
                "Body": 'error\t"quoted"\nsecond line',
                "ResourceAttributes": "{'k8s.namespace.name': 'otel-demo', 'k8s.pod.name': 'payment-abc'}",
            }
        )
    (root / "alerts").mkdir(exist_ok=True)
    atomic_json(
        root / "alerts" / "alerts_in_alerting_state_1.json",
        {
            "data": {
                "alerts": [
                    {
                        "state": "firing",
                        "labels": {
                            "alertname": "ErrorRate",
                            "namespace": "otel-demo",
                            "service_name": "payment",
                        },
                        "annotations": {"description": "Payment error rate is high"},
                    }
                ]
            }
        },
    )
    gt = {
        "groups": [
            {
                "id": "flagd",
                "kind": "ConfigMap",
                "name": "flagd-config",
                "namespace": "otel-demo",
                "root_cause": True,
            }
        ],
        "recommended_actions": CANARY,
    }
    (root / "ground_truth.yaml").write_text(yaml.safe_dump(gt))
    (root / "data.jsonl").write_text(CANARY)
    (root / "grader_results.json").write_text(CANARY)
    return gt


class DataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        fixture(self.root / "source", large=True)
        create_index(self.root / "source", self.root / "index.sqlite3", 8)
        self.index = EvidenceIndex(self.root / "index.sqlite3", 8)

    def tearDown(self):
        self.tmp.cleanup()

    def test_multiline_quoted_tsv_and_python_attributes(self):
        record = self.index.record(
            self.index.refs("otel-demo/Pod/payment-abc", "logs")[0]
        )
        self.assertEqual(record["data"]["Body"], 'error\t"quoted"\nsecond line')
        self.assertEqual(
            record["data"]["ResourceAttributes"]["k8s.pod.name"], "payment-abc"
        )
        self.assertIn("record-1", record["ref"])

    def test_history_identity_cluster_scope_missing_types(self):
        self.assertEqual(len(self.index.refs(ENTITY, "configuration")), 2)
        versions = {
            self.index.record(r)["data"]["Body"]["metadata"]["resourceVersion"]
            for r in self.index.refs(ENTITY, "configuration")
        }
        self.assertEqual(versions, {"1", "2"})
        self.assertIn("cluster/Node/worker", self.index.entities())
        self.assertEqual(self.index.refs(ENTITY, "metrics"), [])
        self.assertIn(
            ("otel-demo/Pod/payment-abc", "otel-demo/Service/payment", "selector"),
            self.index.relationships(),
        )

    def test_pagination_preserves_entire_record_and_utf8(self):
        refs = self.index.refs(ENTITY, "configuration")
        offset, segment = 0, 0
        text = []
        while True:
            page = self.index.page(ENTITY, "configuration", offset, segment)
            self.assertLessEqual(len(dumps(page).encode()), 4096)
            text.append(page["records"][0]["content"])
            if page["next"] is None or page["next"][0] != 0:
                break
            offset, segment = page["next"]
        self.assertEqual(json.loads("".join(text)), self.index.record(refs[0])["data"])
        self.assertGreater(len(text), 1)

    def test_no_labels_fixes_credentials_or_grader_in_index(self):
        with self.index.connect() as db:
            payload = dumps(list(db.execute("SELECT payload FROM records")))
            files = [r[0] for r in db.execute("SELECT DISTINCT file FROM records")]
        self.assertNotIn(CANARY, payload)
        self.assertNotIn("recommended_actions", payload)
        self.assertFalse(
            any(
                "ground_truth" in f or "grader" in f or "data.jsonl" in f for f in files
            )
        )

    def test_scope_traversal_symlink_and_overflow(self):
        with self.assertRaises(ValueError):
            EvidenceIndex(self.root / "index.sqlite3", 2)
        with self.assertRaises(ValueError):
            confined(self.root, "../index.sqlite3")
        (self.root / "link").symlink_to(self.root / "index.sqlite3")
        with self.assertRaises(ValueError):
            confined(self.root, "link")
        with self.assertRaisesRegex(ValueError, "context overflow"):
            bounded_state({"required": "a" * 8192})

    def test_alert_file_at_snapshot_root(self):
        source = self.root / "source"
        (source / "alerts" / "alerts_in_alerting_state_1.json").rename(
            source / "alerts_in_alerting_state_1.json"
        )
        create_index(source, self.root / "root-alerts.sqlite3", 8)
        index = EvidenceIndex(self.root / "root-alerts.sqlite3", 8)
        self.assertEqual(len(index.alert_summaries()), 1)


class ScoreTests(unittest.TestCase):
    def setUp(self):
        self.entities = [
            ENTITY,
            "otel-demo/Service/payment",
            "otel-demo/Pod/payment-abc",
            "otel-demo/Deployment/payment",
            "cluster/Node/worker",
        ]
        self.gt = {
            "groups": [
                {
                    "id": "flagd",
                    "kind": "ConfigMap",
                    "name": "flagd-config",
                    "namespace": "otel-demo",
                    "root_cause": True,
                },
                {
                    "id": "pod",
                    "kind": "Pod",
                    "filter": ["payment-.*"],
                    "namespace": "otel-demo",
                },
                {
                    "id": "service",
                    "kind": "Service",
                    "filter": [r"payment\b"],
                    "namespace": "otel-demo",
                },
            ],
            "aliases": [["pod", "service"]],
        }

    def scored(self, names, gt=None, relations=()):
        return score(
            gt or self.gt,
            {"contributing_factors": [{"name": n} for n in names]},
            self.entities,
            relations,
        )

    def test_correct_incorrect_duplicates_and_layouts(self):
        for gt in [self.gt, {"spec": self.gt}]:
            self.assertEqual(self.scored([ENTITY], gt)["score"], 1)
            self.assertEqual(self.scored([ENTITY, ENTITY], gt)["score"], 1)
            self.assertEqual(self.scored(["otel-demo/Service/payment"], gt)["score"], 0)
            self.assertEqual(self.scored([ENTITY, "no/such/entity"], gt)["score"], 0.5)

    def test_multiple_roots_alias_merging_and_equivalence(self):
        self.gt["groups"][1]["root_cause"] = True
        self.assertEqual(self.scored([ENTITY])["score"], 0)
        self.assertEqual(
            self.scored(
                [ENTITY, "otel-demo/Service/payment", "otel-demo/Pod/payment-abc"]
            )["score"],
            1,
        )
        self.gt["aliases"].append(["flagd", "pod"])
        self.assertEqual(self.scored(["otel-demo/Service/payment"])["score"], 1)

    def test_observed_workload_ownership(self):
        self.gt["groups"][0]["root_cause"] = False
        self.gt["groups"][1]["root_cause"] = True
        rel = [("otel-demo/Pod/payment-abc", "otel-demo/Deployment/payment", "owner")]
        self.assertEqual(
            self.scored(["otel-demo/Deployment/payment"], relations=rel)["score"], 1
        )
        self.assertEqual(self.scored(["otel-demo/Deployment/payment"])["score"], 0)

    def test_ambiguous_unmatched_namespace_kind_and_cluster(self):
        self.gt["groups"].append(
            {
                "id": "ambiguous",
                "kind": "ConfigMap",
                "filter": ["flagd.*"],
                "namespace": "otel-demo",
            }
        )
        self.assertEqual(
            self.scored([ENTITY])["normalization"][0]["status"], "ambiguous"
        )
        for n in [
            "other/ConfigMap/flagd-config",
            "otel-demo/Pod/flagd-config",
            "missing",
            "otel-demo/ConfigMap/nonexistent",
        ]:
            self.assertEqual(
                self.scored([n])["normalization"][0]["status"], "unmatched"
            )
        gt = {
            "groups": [
                {"id": "node", "kind": "Node", "name": "worker", "root_cause": True}
            ]
        }
        self.assertEqual(self.scored(["cluster/Node/worker"], gt)["score"], 1)
        gt["groups"][0]["namespace"] = None
        self.assertEqual(self.scored(["cluster/Node/worker"], gt)["score"], 1)

    def test_service_selector_does_not_invent_alias(self):
        gt = {
            "groups": [
                {
                    "id": "pod",
                    "kind": "Pod",
                    "filter": ["payment-.*"],
                    "namespace": "otel-demo",
                    "root_cause": True,
                }
            ]
        }
        relationships = [
            ("otel-demo/Pod/payment-abc", "otel-demo/Service/payment", "selector")
        ]
        self.assertEqual(
            self.scored(["otel-demo/Service/payment"], gt, relationships)["score"], 0
        )

    def test_named_chaos_schedule_spawned_resource(self):
        gt = {
            "groups": [
                {
                    "id": "network",
                    "kind": "NetworkChaos",
                    "filter": ["network-delay.*"],
                    "namespace": "chaos-mesh",
                    "root_cause": True,
                }
            ]
        }
        name = "chaos-mesh/Schedule/network-delay"
        result = score(gt, {"contributing_factors": [{"name": name}]}, [name])
        self.assertEqual(result["score"], 1)

    def test_duplicate_ground_truth_definitions_preserve_root_flags(self):
        self.gt["groups"].append({**self.gt["groups"][0], "root_cause": False})
        self.assertEqual(self.scored([ENTITY])["score"], 1)

    def test_release_leading_wildcard_filter_repair_is_recorded(self):
        gt = {
            "groups": [
                {"id": "node", "kind": "Node", "filter": ["*.*"], "root_cause": True}
            ]
        }
        result = self.scored(["cluster/Node/worker"], gt)
        self.assertEqual(result["score"], 1)
        self.assertEqual(result["filter_repairs"][0]["regex"], ".*.*")


class LedgerTests(unittest.TestCase):
    def test_previous_campaign_spending_is_frozen_and_limits_new_reservations(self):
        with tempfile.TemporaryDirectory() as root:
            previous = Ledger(Path(root) / "previous.sqlite3", budget=20_000_000_000)
            previous.reserve("old")
            snapshot = Ledger.prior_snapshot(previous.path)
            new = Ledger(Path(root) / "new.sqlite3", budget=64000 * 42 * 2 - 1)
            new.inherit(snapshot)
            new.inherit(snapshot)
            with self.assertRaises(SpendingLimit):
                new.reserve("rerun")
            reopened = Ledger(new.path)
            self.assertEqual(
                reopened.stats()["prior_accounted_nanodollars"], 64000 * 42
            )
            self.assertEqual(reopened.stats()["prior_calls"], 1)
            self.assertEqual(reopened.stats()["prior_unknown_calls"], 1)
            self.assertEqual(Ledger.prior_snapshot(previous.path), snapshot)
            previous.reserve("another")
            with self.assertRaises(ValueError):
                new.inherit(snapshot)

    def test_unfunded_call_is_reported_as_budget_failure_without_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            ledger = Ledger(Path(root) / "ledger.sqlite3", budget=1)
            with self.assertRaises(SpendingLimit):
                ledger.reserve("trial")
            events = [
                {"kind": "provider_failure", "payload": {"error_type": "SpendingLimit"}}
            ]
            self.assertEqual(failure_category(events, None, ledger, "trial"), "budget")
            self.assertEqual(ledger.stats()["calls"], 0)

    def test_reservation_unknown_outcome_reconciliation_and_ceiling(self):
        with tempfile.TemporaryDirectory() as root:
            ledger = Ledger(Path(root) / "ledger.sqlite3", budget=64000 * 42)
            call = ledger.reserve("one")
            with self.assertRaises(SpendingLimit):
                ledger.reserve("two")
            ledger.failure(call, TimeoutError())
            self.assertEqual(ledger.stats()["unknown_calls"], 1)
            ledger = Ledger(Path(root) / "ledger.sqlite3", budget=64000 * 42)
            self.assertEqual(ledger.stats()["accounted_nanodollars"], 64000 * 42)
            ledger.reconcile(call, {"input_tokens": 1, "output_tokens": 200}, 0.2)
            self.assertEqual(ledger.stats()["accounted_nanodollars"], 42)
            with self.assertRaises(ValueError):
                ledger.reconcile(call, {"input_tokens": 1, "output_tokens": 0}, 0)

    def test_100_call_limit_includes_waits_and_usage_overflow_retained(self):
        with tempfile.TemporaryDirectory() as root:
            ledger = Ledger(Path(root) / "ledger.sqlite3", call_limit=100)
            for i in range(100):
                cid = ledger.reserve("trial")
                if i == 99:
                    with self.assertRaises(ValueError):
                        ledger.reconcile(
                            cid, {"input_tokens": 64001, "output_tokens": 1}, 0
                        )
                else:
                    ledger.reconcile(cid, {"input_tokens": 10, "output_tokens": 1}, 0)
            with self.assertRaises(CallLimit):
                ledger.reserve("trial")
            self.assertEqual(ledger.stats()["unknown_calls"], 1)


class HarnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.gt = fixture(self.base / "dataset" / "sre" / "Scenario-8")
        meta = create_index(
            self.base / "dataset" / "sre" / "Scenario-8",
            self.base / "indexes" / "Scenario-8.sqlite3",
            8,
        )
        self.prepared = {
            "dataset": str(self.base / "dataset"),
            "fingerprint": "fixture",
            "indexes": {
                "Scenario-8": {
                    **meta,
                    "path": str(self.base / "indexes" / "Scenario-8.sqlite3"),
                }
            },
        }
        self.campaign = self.base / "campaign"
        self.campaign.mkdir()
        self.calls = []
        self.env = patch.dict(
            os.environ, {"TYPESAFE_API_KEY": "test-private-credential"}
        )
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def simulated(
        self,
        *,
        outage=False,
        wrong=False,
        retry_statuses=(),
        invalid=False,
        full_transient=False,
    ):
        original = httpx.AsyncClient
        transient = iter(retry_statuses)
        full_failed = False

        def handle(request):
            nonlocal full_failed
            payload = json.loads(request.content)
            self.calls.append(payload)
            if (
                full_transient
                and not full_failed
                and payload["state"]["incident_id"].startswith("full-")
            ):
                full_failed = True
                return httpx.Response(
                    520, json={"error": CANARY}, headers={"retry-after": "60"}
                )
            status = next(transient, None)
            if status:
                return httpx.Response(status, json={"error": CANARY})
            if outage:
                return httpx.Response(503, json={"error": CANARY})
            criteria = payload["questions"]["next_action"]["criteria"]
            w = payload["state"]["observations"][-1]["payload"]
            target = "otel-demo/Service/payment" if wrong else ENTITY
            desired = (
                "focus"
                if not w["focus"]
                else "inspect"
                if not w["evidence"]["records"]
                else "nominate"
                if not w["nominations"]
                else "submit"
            )
            label = next(
                k
                for k, v in criteria.items()
                if isinstance(v, dict)
                and v["args"]["operation"] == desired
                and (
                    desired not in {"focus", "inspect"} or v["args"]["entity"] == target
                )
                and (desired != "inspect" or v["args"]["kind"] == "configuration")
            )
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13.0",
                    "answers": {
                        "next_action": {
                            "type": "choice",
                            "choice": label,
                            "confidence": CANARY if invalid else 1.0,
                            "probabilities": {
                                k: 1.0 if k == label else 0.0 for k in criteria
                            },
                        }
                    },
                    "usage": {"input_tokens": 100, "output_tokens": 10},
                },
            )

        return patch.object(
            httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: original(
                **kwargs, transport=httpx.MockTransport(handle)
            ),
        )

    async def test_complete_submission_score_and_resume_no_calls(self):
        with self.simulated():
            row = await run_trial(
                self.campaign, self.prepared, 8, "pilot-s8", 42, "pilot", self.base
            )
            count = len(self.calls)
            resumed = await run_trial(
                self.campaign, self.prepared, 8, "pilot-s8", 42, "pilot", self.base
            )
        self.assertEqual(row["category"], "completed")
        self.assertEqual(row["score"], 1)
        self.assertEqual(resumed, row)
        self.assertEqual(count, 4)
        self.assertEqual(len(self.calls), count)
        encoded = dumps(self.calls)
        self.assertNotIn(CANARY, encoded)
        self.assertNotIn("test-private-credential", encoded)
        self.assertNotIn("ground_truth", encoded)
        self.assertTrue(
            all(
                len(p["questions"]["next_action"]["criteria"]) <= 20 for p in self.calls
            )
        )

    async def test_retries_reserve_each_attempt_and_survive_success_and_reopen(self):
        async def fast_sleep(seconds):
            pass

        stderr = io.StringIO()
        with (
            self.simulated(retry_statuses=[429, 529]),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("sys.stderr", stderr),
        ):
            row = await run_trial(
                self.campaign, self.prepared, 8, "retries", 42, "full", self.base
            )
        self.assertEqual(row["category"], "completed")
        self.assertEqual(row["calls"], 6)
        self.assertEqual(row["retry_calls"], 2)
        self.assertEqual(row["unknown_calls"], 2)
        self.assertEqual(row["accounted_nanodollars"], 2 * 64000 * 42 + 400 * 42)
        reopened = Ledger(self.campaign / "spending.sqlite3")
        details = reopened.diagnostics("retries")
        self.assertEqual([d["failure"]["http_status"] for d in details], [429, 529])
        self.assertEqual([d["attempt"] for d in details], [1, 2])
        self.assertNotIn(CANARY, json.dumps(details) + stderr.getvalue())

    async def test_invalid_answer_keeps_known_usage_and_specific_persistent_failure(
        self,
    ):
        stderr = io.StringIO()
        with self.simulated(invalid=True), patch("sys.stderr", stderr):
            row = await run_trial(
                self.campaign, self.prepared, 8, "invalid", 42, "full", self.base
            )
        self.assertEqual(row["category"], "provider")
        self.assertEqual(row["calls"], 1)
        self.assertEqual(row["unknown_calls"], 0)
        self.assertEqual(row["accounted_nanodollars"], 100 * 42)
        self.assertEqual(row["failure_details"][0]["failure"]["code"], "answer_schema")
        self.assertNotIn(CANARY, json.dumps(row) + stderr.getvalue())

    async def test_retry_cannot_exceed_spending(self):
        async def fast_sleep(seconds):
            pass

        ledger = Ledger(self.campaign / "spending.sqlite3", budget=64000 * 42)
        with (
            self.simulated(retry_statuses=[429]),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("sys.stderr", io.StringIO()),
        ):
            row = await run_trial(
                self.campaign, self.prepared, 8, "budget", 42, "full", self.base
            )
        self.assertEqual(row["category"], "budget")
        self.assertEqual(row["failure"]["code"], "spending_limit")
        self.assertEqual(row["calls"], 1)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(ledger.stats()["accounted_nanodollars"], ledger.budget)

    async def test_retry_cannot_exceed_per_trial_call_limit(self):
        async def fast_sleep(seconds):
            pass

        ledger = Ledger(self.campaign / "spending.sqlite3", call_limit=100)
        for _ in range(98):
            cid = ledger.reserve("call-cap")
            ledger.reconcile(cid, {"input_tokens": 1, "output_tokens": 0}, 0)
        with (
            self.simulated(retry_statuses=[429, 529]),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("sys.stderr", io.StringIO()),
        ):
            row = await run_trial(
                self.campaign, self.prepared, 8, "call-cap", 42, "full", self.base
            )
        self.assertEqual(row["category"], "call_limit")
        self.assertEqual(row["failure"]["code"], "call_limit")
        self.assertEqual(row["calls"], 100)
        self.assertEqual(len(self.calls), 2)

    async def test_resolved_is_not_diagnostic_correctness(self):
        with self.simulated(wrong=True):
            row = await run_trial(
                self.campaign, self.prepared, 8, "wrong", 42, "full", self.base
            )
        self.assertEqual(row["terminal"]["outcome"], "resolved")
        self.assertEqual(row["category"], "completed")
        self.assertEqual(row["score"], 0)

    async def test_provider_outage_exhausts_retries_without_submission_unknown_billing(
        self,
    ):
        with self.simulated(outage=True):
            row = await run_trial(
                self.campaign, self.prepared, 8, "outage", 42, "pilot", self.base
            )
        self.assertEqual(row["category"], "provider")
        self.assertEqual(row["score"], 0)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(row["unknown_calls"], 3)
        self.assertFalse((self.campaign / "diagnoses" / "outage.json").exists())

    async def test_scoped_candidates_stale_revision_and_arguments(self):
        settings = Settings(
            index=str(self.base / "indexes" / "Scenario-8.sqlite3"),
            scenario=8,
            trial="one",
            output=str(self.campaign / "diagnoses" / "one.json"),
        )
        context = AppContext({}, Limits(), self.base)
        normalizer = Normalizer(settings, context)
        with self.assertRaises(ValueError):
            await normalizer.normalize(
                RawIncident(
                    source_id="itbench-aa",
                    event_id="one",
                    payload={"scenario": 2, "trial": "one"},
                )
            )
        state = await normalizer.normalize(
            RawIncident(
                source_id="itbench-aa",
                event_id="one",
                payload={"scenario": 8, "trial": "one"},
            )
        )
        plugin = Plugin(settings, context)
        candidate = (await plugin.candidates(state))[0]
        changed = candidate.model_copy(deep=True)
        changed.args["entity"] = "other/Pod/entity"
        self.assertFalse((await plugin.validate(changed, state)).allowed)
        with self.assertRaises(ValueError):
            await plugin.execute(changed, state)
        await plugin.execute(candidate, state)
        self.assertFalse((await plugin.validate(candidate, state)).allowed)

    async def test_provider_timeout_retains_reservation(self):
        settings = Settings(
            index=str(self.base / "indexes" / "Scenario-8.sqlite3"),
            scenario=8,
            trial="timeout",
            output=str(self.campaign / "diagnoses" / "timeout.json"),
        )
        context = AppContext({}, Limits(), self.base)
        state = await Normalizer(settings, context).normalize(
            RawIncident(
                source_id="itbench-aa",
                event_id="timeout",
                payload={"scenario": 8, "trial": "timeout"},
            )
        )
        request = DecisionRequest(
            incident_id="timeout",
            state=state,
            candidates=await Plugin(settings, context).candidates(state),
        )
        provider = BudgetedProvider(
            ProviderSettings(
                ledger=str(self.campaign / "spending.sqlite3"),
                trial="timeout",
                timeout_seconds=0.03,
            ),
            context,
        )

        async def stalled(request):
            self.calls.append(request)
            await asyncio.sleep(1)

        original = httpx.AsyncClient
        with patch.object(
            httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: original(
                **kwargs, transport=httpx.MockTransport(stalled)
            ),
        ):
            with self.assertRaises((TimeoutError, JevError)):
                await provider.decide(request)
        await asyncio.sleep(0.03)
        self.assertEqual(provider.ledger.stats()["calls"], 1)
        self.assertEqual(provider.ledger.stats()["unknown_calls"], 1)
        self.assertEqual(len(self.calls), 1)
        self.assertIn(
            provider.ledger.diagnostics()[0]["failure"]["code"],
            {"request_cancelled", "provider_deadline"},
        )

    async def test_submission_rejects_foreign_evidence(self):
        settings = Settings(
            index=str(self.base / "indexes" / "Scenario-8.sqlite3"),
            scenario=8,
            trial="ownership",
            output=str(self.campaign / "diagnoses" / "ownership.json"),
        )
        plugin = Plugin(settings, AppContext({}, Limits(), self.base))
        rid = plugin.index.refs("otel-demo/Pod/payment-abc", "logs")[0]
        with self.assertRaisesRegex(ValueError, "ownership"):
            plugin.diagnosis({ENTITY: {"reason": "configuration", "records": [rid]}})
        self.assertFalse(plugin.output.exists())

    async def test_interruption_record_is_not_replayed(self):
        atomic_json(
            self.campaign / "trials" / "interrupted.json",
            {
                "trial": "interrupted",
                "phase": "full",
                "scenario": 8,
                "status": "running",
                "dataset_fingerprint": "fixture",
                "configuration_fingerprint": fingerprint(DEFAULTS),
            },
        )
        with self.simulated():
            row = await run_trial(
                self.campaign, self.prepared, 8, "interrupted", 42, "full", self.base
            )
        self.assertEqual(row["category"], "interrupted")
        self.assertEqual(row["score"], 0)
        self.assertEqual(len(self.calls), 0)

    async def test_durable_submission_scored_without_provider(self):
        with self.simulated():
            row = await run_trial(
                self.campaign, self.prepared, 8, "durable", 42, "full", self.base
            )
        path = self.campaign / "trials" / "durable.json"
        row["status"] = "running"
        atomic_json(path, row)
        count = len(self.calls)
        with self.simulated():
            resumed = await run_trial(
                self.campaign, self.prepared, 8, "durable", 42, "full", self.base
            )
        self.assertEqual(resumed["score"], 1)
        self.assertEqual(resumed["category"], "interrupted_with_submission")
        self.assertEqual(len(self.calls), count)

    async def test_full_offline_campaign_and_pilot_separation(self):
        # Use 40 tiny snapshots to exercise the exact 10 + 120 schedule offline.
        root = self.base / "offline"
        root.mkdir()
        scenarios = sorted(set(PILOT) | set(range(1, 33)))
        scenarios = scenarios[:40]
        while len(scenarios) < 40:
            scenarios.append(max(scenarios) + 1)
        indexes = {}
        for s in scenarios:
            source = self.base / "dataset" / "sre" / f"Scenario-{s}"
            fixture(source)
            path = root / "indexes" / f"Scenario-{s}.sqlite3"
            meta = create_index(source, path, s)
            indexes[f"Scenario-{s}"] = {
                **meta,
                "path": str(path),
                "sha256": file_hash(path),
            }
        prepared = {
            "dataset": str(self.base / "dataset"),
            "revision": REVISION,
            "license": "CC-BY-4.0",
            "scenarios": sorted(scenarios),
            "indexes": indexes,
            "files": {},
        }
        prepared["fingerprint"] = fingerprint(prepared)
        atomic_json(root / "prepared.json", prepared)
        (self.base / "uv.lock").write_text("offline lock")
        (self.base / "pyproject.toml").write_text("# offline fixture")
        with (
            self.simulated(),
            patch(
                "urllib.request.urlopen",
                return_value=io.BytesIO(
                    b"0.042; 64k tokens per request; Output tokens are free"
                ),
            ),
            patch("builtins.print"),
        ):
            campaign = await run_campaign(root, "offline-campaign", base=self.base)
        summary = report(campaign)
        self.assertEqual(summary["full_trials"], 120)
        self.assertEqual(summary["pilot_trials"], 10)
        self.assertEqual(summary["mean_score"], 1)
        self.assertEqual(summary["ledger"]["calls"], 520)
        self.assertTrue(summary["complete"])
        with self.simulated(), patch("builtins.print"):
            await run_campaign(root, "offline-campaign", base=self.base)
        self.assertEqual(len(self.calls), 520)

        async def no_cooldown(seconds):
            pass

        with (
            self.simulated(outage=True),
            patch(
                "urllib.request.urlopen",
                return_value=io.BytesIO(
                    b"0.042; 64k tokens per request; Output tokens are free"
                ),
            ),
            patch("dowser.bench.asyncio.sleep", side_effect=no_cooldown),
            patch("builtins.print"),
        ):
            failed = await run_campaign(
                root, "failed-pilot", phase="pilot", base=self.base
            )
        failed_summary = report(failed)
        self.assertEqual(failed_summary["pilot_trials"], 10)
        self.assertEqual(failed_summary["full_trials"], 0)
        self.assertEqual(failed_summary["pilot_categories"], {"provider": 10})
        self.assertEqual(len(self.calls), 550)

        with (
            self.simulated(full_transient=True),
            patch(
                "urllib.request.urlopen",
                return_value=io.BytesIO(
                    b"0.042; 64k tokens per request; Output tokens are free"
                ),
            ),
            patch("dowser.bench.asyncio.sleep", side_effect=no_cooldown),
            patch("builtins.print"),
        ):
            resilient = await run_campaign(root, "transient-full", base=self.base)
        resilient_summary = report(resilient)
        self.assertEqual(resilient_summary["full_trials"], 120)
        self.assertEqual(resilient_summary["pilot_trials"], 10)
        self.assertAlmostEqual(resilient_summary["mean_score"], 119 / 120)
        self.assertEqual(
            resilient_summary["failure_categories"], {"provider": 1, "completed": 119}
        )
        self.assertEqual(resilient_summary["ledger"]["calls"], 517)
        self.assertEqual(resilient_summary["ledger"]["unknown_calls"], 1)
        self.assertTrue(resilient_summary["complete"])
        prepared["fingerprint"] = "different"
        atomic_json(root / "prepared.json", prepared)
        with self.assertRaises(ValueError):
            await run_campaign(root, "offline-campaign", base=self.base)


if __name__ == "__main__":
    unittest.main()

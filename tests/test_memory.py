"""Behavioral contracts for durable memory, semantic repetition and recall."""

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from test_itbench_aa import ENTITY, fixture

from dowser.bench import trial_config
from dowser.bench_data import create_index
from dowser.bench_ledger import Ledger
from dowser.config import Application
from dowser.contracts import AppContext
from dowser.core import ContextSettings, DefaultContextBuilder, DefaultRegistry
from dowser.memory import action_identity
from dowser.models import (
    ActionCandidate,
    ActionIdentity,
    DecisionResult,
    IncidentState,
    Limits,
    MemoryFact,
    MemoryScope,
    Observation,
    ParseResult,
    RawIncident,
    Resource,
)
from dowser.store import SQLiteStore
from dowser.validate_memory import FAMILIES, focused_request, run_focused
from plugins.itbench_aa import Normalizer, Plugin, Settings, working
from plugins.jev import JevProvider, JevSettings


def state(incident="one", partition="tenant-a", resource="payment"):
    return IncidentState(
        incident_id=incident,
        alert={"service": "payment"},
        desired_state={"healthy": True},
        resources=[Resource(id=resource, platform="fixture", platform_version="1")],
        memory_scope=MemoryScope(namespace="tests", partition=partition)
        if partition
        else None,
    )


def candidate(resource="payment", **args):
    return ActionCandidate(
        id="read",
        tool="fixture.read",
        plugin_version="1",
        args=args,
        description="Inspect dependency",
        effect="read_only",
        resources=[resource],
        verification="health",
    )


async def record_action(store, s, c, status="failed", identity=None, finish=True):
    await store.append(
        s.incident_id,
        "execution_started",
        {
            "execution_id": c.id,
            "candidate": c.model_dump(mode="json"),
            "identity": (identity or action_identity(c)).model_dump(),
        },
    )
    if finish:
        await store.append(
            s.incident_id,
            "execution_result",
            {
                "execution_id": c.id,
                "tool": c.tool,
                "status": status,
                "detail": "dependency probe timed out",
                "parse": {
                    "status": "skipped",
                    "parser_version": "tests/1",
                    "reason": "no usable output",
                },
            },
        )


class MemoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "history.sqlite3"
        self.store = SQLiteStore(self.path)

    async def asyncTearDown(self):
        await self.store.aclose()
        self.tmp.cleanup()

    async def fact(self, s, key, value):
        await self.store.append(
            s.incident_id,
            "parse_outcome",
            {
                "execution_id": "fixture",
                "parse": ParseResult(
                    status="valid",
                    parser_version="tests/1",
                    memory_facts=[
                        MemoryFact(
                            key=key, resource_id=s.resources[0].id, payload=value
                        )
                    ],
                ).model_dump(mode="json"),
            },
        )

    async def test_earlier_finding_survives_navigation_later_observation_and_restart(
        self,
    ):
        s = state()
        await self.store.ingest(s)
        await self.fact(s, "db", {"unreachable": True})
        s.observations = [
            Observation(
                resource_id="payment", kind="navigation", payload={"page": "cache"}
            )
        ]
        await self.store.aclose()
        self.store = SQLiteStore(self.path)
        request = await DefaultContextBuilder(
            ContextSettings(recent_outcomes=0), self.store
        ).build(s, [candidate()])
        self.assertEqual(request.state.attempts, [])
        self.assertEqual(request.memory.facts[0]["payload"], {"unreachable": True})
        self.assertTrue(request.memory.facts[0]["event_ref"].startswith("one:"))

    async def test_trim_preserves_progress_and_essential_findings(self):
        s = state()
        await self.store.ingest(s)
        for i in range(15):
            await self.fact(s, f"fact-{i}", {"finding": i})
        s.attempts = [{"old_output": "x" * 100}]
        builder = DefaultContextBuilder(ContextSettings(), self.store)
        request = await builder.build(s, [candidate()])
        while trimmed := await builder.trim(request):
            request = trimmed
        self.assertEqual(request.memory.progress["facts"], 15)
        self.assertEqual(len(request.memory.facts), 2)
        self.assertEqual(request.memory.facts[0]["payload"], {"finding": 14})
        self.assertEqual(request.memory.progress["omitted_facts"], 13)

    async def test_failure_reason_and_unknown_outcome_survive_restart(self):
        s = state()
        await self.store.ingest(s)
        await record_action(self.store, s, candidate())
        c = candidate()
        c.id = "unknown"
        await record_action(self.store, s, c, finish=False)
        await self.store.aclose()
        self.store = SQLiteStore(self.path)
        memory = await self.store.memory(s, [])
        self.assertEqual(memory["progress"]["outcomes"]["unknown"], 1)
        self.assertEqual(memory["progress"]["outcomes"]["failed"], 1)
        self.assertEqual(
            memory["actions"][1]["detail"]["execution"], "dependency probe timed out"
        )

    async def test_contradictory_findings_keep_both_values_and_provenance(self):
        s = state()
        await self.store.ingest(s)
        await self.fact(s, "db", {"reachable": True})
        await self.fact(s, "db", {"reachable": False})
        memory = await self.store.memory(s, [])
        self.assertEqual(memory["progress"]["contradictions"], ["db"])
        self.assertEqual({f["status"] for f in memory["facts"]}, {"contradicted"})
        self.assertEqual(len({f["event_ref"] for f in memory["facts"]}), 2)

    async def test_related_alert_scope_and_freshness(self):
        previous, current = state("previous"), state("current")
        await self.store.ingest(previous)
        await self.fact(previous, "db", {"reachable": False})
        await record_action(self.store, previous, candidate())
        memory = await self.store.memory(current, [])
        self.assertTrue(memory["historical"])
        self.assertEqual(memory["facts"][0]["freshness"], "revalidation_required")
        self.assertTrue(memory["actions"][0]["historical"])
        self.assertEqual(memory["progress"]["current_incident_actions"], 0)
        for other in (
            state("other", "tenant-b"),
            state("other", resource="billing"),
            state("other", partition=None),
        ):
            self.assertEqual((await self.store.memory(other, []))["facts"], [])

    async def test_benchmark_trial_scopes_never_share_evidence(self):
        first, second = state("pilot-s8"), state("full-r1-s8")
        first.memory_scope = MemoryScope(
            namespace="itbench-aa", partition=first.incident_id
        )
        second.memory_scope = MemoryScope(
            namespace="itbench-aa", partition=second.incident_id
        )
        await self.store.ingest(first)
        await self.fact(first, "db", {"reachable": False})
        self.assertEqual((await self.store.memory(second, []))["facts"], [])

    async def test_semantic_attempts_persist_and_evidence_version_allows_reinspection(
        self,
    ):
        s, ident = (
            state(),
            ActionIdentity(key="stable", evidence_version="v1", decision_state="focus"),
        )
        await self.store.ingest(s)
        await record_action(self.store, s, candidate(revision=1), identity=ident)
        await self.store.aclose()
        self.store = SQLiteStore(self.path)
        self.assertEqual(await self.store.action_count(s.incident_id, ident), 1)
        self.assertEqual(
            await self.store.action_count(
                s.incident_id, ident.model_copy(update={"evidence_version": "v2"})
            ),
            0,
        )

    async def test_legacy_schema_migrates_without_changing_events_or_executing(self):
        s = state()
        await self.store.ingest(s)
        await record_action(self.store, s, candidate())
        before = [
            e.model_dump(mode="json") for e in await self.store.history(s.incident_id)
        ]
        await self.store.aclose()
        with sqlite3.connect(self.path) as db:
            for table in ("memory_facts", "memory_actions", "memory_scopes"):
                db.execute(f"DROP TABLE {table}")
            db.execute("PRAGMA user_version=1")
        self.store = SQLiteStore(self.path)
        self.assertEqual(
            [
                e.model_dump(mode="json")
                for e in await self.store.history(s.incident_id)
            ],
            before,
        )
        self.assertEqual((await self.store.memory(s, []))["progress"]["actions"], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.connection.execute("DELETE FROM events")

    async def test_legacy_store_fallback_retains_outcomes(self):
        s = state()
        await self.store.ingest(s)
        await record_action(self.store, s, candidate())

        class Legacy:
            history = self.store.history

        request = await DefaultContextBuilder(ContextSettings(), Legacy()).build(
            s, [candidate()]
        )
        self.assertEqual(request.memory.actions[0]["status"], "failed")

    async def test_memory_is_in_native_jev_payload(self):
        s = state()
        await self.store.ingest(s)
        await self.fact(s, "db", {"reachable": False})
        request = await DefaultContextBuilder(ContextSettings(), self.store).build(
            s, [candidate()]
        )
        provider = JevProvider(
            JevSettings(), AppContext({}, Limits(), Path(self.tmp.name))
        )
        payload, _ = provider.prepare(request)
        self.assertEqual(
            payload["state"]["investigation_memory"]["facts"][0]["payload"],
            {"reachable": False},
        )


class RecallTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_loop_cumulative_memory_and_repeat_gate(self):
        for sequence in (
            ("focus", "inspect", "browse", "focus", "recall", "nominate", "submit"),
            ("focus", "inspect", "recall", "escalate"),
        ):
            with self.subTest(sequence=sequence), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                fixture(root / "source")
                create_index(root / "source", root / "index.sqlite3", 8)
                prepared = {
                    "indexes": {"Scenario-8": {"path": str(root / "index.sqlite3")}}
                }
                requests = []

                async def decide(provider, request):
                    requests.append(request.model_copy(deep=True))
                    op = sequence[len(requests) - 1]
                    if op == "escalate":
                        self.assertTrue(
                            any(
                                c.args.get("operation") == "recall"
                                and c.args["kind"] == "configuration"
                                for c in request.candidates
                            )
                        )
                        return DecisionResult(
                            operation="escalate", reason="investigation exhausted"
                        )
                    choice = next(
                        c
                        for c in request.candidates
                        if c.args.get("operation") == op
                        and (op != "focus" or c.args["entity"] == ENTITY)
                        and (
                            op not in {"inspect", "recall"}
                            or (
                                c.args["kind"] == "configuration"
                                and c.args["entity"] == ENTITY
                            )
                        )
                        and (op != "nominate" or c.args["reason"] == "configuration")
                    )
                    return DecisionResult(operation="select", candidate_id=choice.id)

                with patch.object(JevProvider, "decide", decide):
                    config = trial_config(root / "campaign", prepared, 8, "loop", 42)
                    if sequence[-1] == "escalate":
                        config.decision_provider.settings["allow_escalation"] = True
                        config.decision_provider.settings["escalation_policy"][
                            "settings"
                        ]["enabled"] = True
                        config.normalizer.settings["allow_escalation"] = True
                    async with Application(config, root) as app:
                        normalizer = app.services["normalizer"]
                        s = await normalizer.normalize(
                            RawIncident(
                                source_id="itbench-aa",
                                event_id="loop",
                                payload={"scenario": 8, "trial": "loop"},
                            )
                        )
                        plugin = app.services["tool_registry"].plugins[0]
                        with patch.object(
                            plugin.index, "page", wraps=plugin.index.page
                        ) as pages:
                            result = await app.services["incident_loop"].run(s)
                            self.assertEqual(pages.call_count, 1)
                        self.assertEqual(
                            result.outcome,
                            "resolved" if sequence[-1] == "submit" else "escalated",
                        )
                        self.assertEqual(
                            requests[-1].memory.progress["actions"], len(sequence) - 1
                        )
                        self.assertTrue(requests[-1].memory.facts)
                        if sequence[-1] == "submit":
                            self.assertTrue(
                                requests[3].memory.facts,
                                "navigation erased prior findings",
                            )
                        else:
                            events = await app.services["event_store"].history("loop")
                            self.assertFalse(
                                any(
                                    e.kind == "validation_outcome"
                                    and "identical action"
                                    in e.payload.get("reason", "")
                                    for e in events
                                )
                            )

    async def test_revision_identity_and_cached_page_recall(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            fixture(root / "source")
            create_index(root / "source", root / "index.sqlite3", 8)
            store = SQLiteStore(root / "history.sqlite3")
            self.addAsyncCleanup(store.aclose)
            settings = Settings(
                index=str(root / "index.sqlite3"),
                scenario=8,
                trial="test",
                output=str(root / "submission.json"),
            )
            context = AppContext({"event_store": store}, Limits(), root)
            s = await Normalizer(settings, context).normalize(
                RawIncident(
                    source_id="itbench-aa",
                    event_id="test",
                    payload={"scenario": 8, "trial": "test"},
                )
            )
            await store.ingest(s)
            plugin = Plugin(settings, context)
            registry = DefaultRegistry([plugin])

            async def execute(c):
                result = await plugin.execute(c, s)
                parsed = await registry.parse(c, result)
                s.observations.extend(parsed.observations)
                return parsed

            focus = next(
                c
                for c in await plugin.candidates(s)
                if c.args.get("operation") == "focus" and c.args["entity"] == ENTITY
            )
            await execute(focus)
            read = next(
                c
                for c in await plugin.candidates(s)
                if c.args.get("operation") == "inspect"
                and c.args["kind"] == "configuration"
            )
            ident = await plugin.action_identity(read, s)
            changed = read.model_copy(deep=True)
            changed.args["revision"] += 100
            self.assertEqual(await plugin.action_identity(changed, s), ident)
            parsed = await execute(read)
            self.assertTrue(parsed.memory_facts)
            expected = json.loads(json.dumps(working(s)["evidence"]))
            recall = next(
                c
                for c in await plugin.candidates(s)
                if c.args.get("operation") == "recall"
                and c.args["kind"] == "configuration"
            )
            with patch.object(
                plugin.index,
                "page",
                side_effect=AssertionError("external read repeated"),
            ):
                await execute(recall)
            self.assertEqual(working(s)["evidence"], expected)


class UnlimitedAccountingTests(unittest.TestCase):
    def test_no_ceiling_and_no_default_call_limit_with_persistent_accounting(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "spending.sqlite3"
            ledger = Ledger(path, price=250000)
            for _ in range(101):
                ledger.reserve("trial")
            self.assertGreater(ledger.stats()["accounted_nanodollars"], 15_000_000_000)
            self.assertIsNone(ledger.budget)
            self.assertIsNone(ledger.call_limit)
            reopened = Ledger(path, price=250000)
            self.assertEqual(reopened.stats()["calls"], 101)


class LiveFixtureTests(unittest.IsolatedAsyncioTestCase):
    async def test_validation_runner_native_http_accounting_controls_and_no_replay(
        self,
    ):
        original = httpx.AsyncClient
        calls = []

        def handle(request):
            body = json.loads(request.content)
            calls.append(body)
            memory = json.dumps(body["state"].get("investigation_memory", {}))
            label = (
                "c1" if "wrong hostname" in memory or "malformed" in memory else "c0"
            )
            criteria = body["questions"]["next_action"]["criteria"]
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13.0",
                    "answers": {
                        "next_action": {
                            "type": "choice",
                            "choice": label,
                            "probabilities": {k: float(k == label) for k in criteria},
                            "confidence": 1,
                        }
                    },
                    "usage": {"input_tokens": 100, "output_tokens": 10},
                },
            )

        def client(**kwargs):
            return original(**kwargs, transport=httpx.MockTransport(handle))

        with (
            tempfile.TemporaryDirectory() as root,
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "offline-only"}),
            patch("httpx.AsyncClient", client),
            patch(
                "dowser.validate_memory.freeze_price_source",
                return_value="offline-price-hash",
            ),
            patch("builtins.print"),
        ):
            directory = Path(root) / "focused"
            summary = await run_focused(directory, Path(__file__).resolve().parents[1])
            self.assertTrue(summary["passed"])
            self.assertEqual(summary["cases"], 45)
            self.assertEqual(summary["ledger"]["calls"], 45)
            self.assertEqual(summary["ledger"]["input_tokens"], 4500)
            self.assertEqual(
                sum("investigation_memory" in p["state"] for p in calls), 30
            )
            self.assertEqual(len(list((directory / "requests").glob("*.json"))), 45)
            with self.assertRaises(ValueError):
                await run_focused(directory, Path(__file__).resolve().parents[1])
            self.assertEqual(len(calls), 45)

    async def test_pairs_keep_current_state_and_candidates_identical(self):
        with tempfile.TemporaryDirectory() as root:
            for family in FAMILIES:
                first = await focused_request(
                    Path(root) / f"{family}-a.sqlite3", family, "network", "paired"
                )
                second = await focused_request(
                    Path(root) / f"{family}-b.sqlite3",
                    family,
                    "configuration",
                    "paired",
                )
                self.assertEqual(first.state, second.state)
                self.assertEqual(first.candidates, second.candidates)
                self.assertNotEqual(first.memory, second.memory)
                self.assertIs(first.memory.historical, family == "related_alert")


if __name__ == "__main__":
    unittest.main()

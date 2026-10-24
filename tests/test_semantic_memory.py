"""Offline semantic memory: actual RAM Qdrant, mocked embeddings and model transport."""

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from dowser.bench import DEFAULTS, run_trial, trial_config
from dowser.bench_data import create_index
from dowser.bench_ledger import Ledger
from dowser.catalogue import decode_decision_input
from dowser.config import validate_config
from dowser.contracts import AppContext
from dowser.core import ContextSettings, DefaultContextBuilder
from dowser.diagnostics import DiagnosticError
from dowser.evaluate_memory import (
    clustered_difference,
    compare,
    evaluate_retrieval,
    retrieval_metrics,
)
from dowser.models import (
    ActionCandidate,
    IncidentState,
    Limits,
    MemoryFact,
    MemoryQueryIntent,
    MemoryScope,
    Observation,
    ParseResult,
)
from dowser.semantic import chunks, query_intents, relationships
from dowser.semantic_store import (
    EmbeddingUnavailable,
    OpenAIEmbedder,
    SemanticSettings,
    SemanticStore,
)
from dowser.tokens import estimate
from plugins.semantic_memory import SemanticMemoryPlugin

HAS_QDRANT = importlib.util.find_spec("qdrant_client") is not None


def state(name="trial"):
    return IncidentState(
        incident_id=name,
        alert={"symptom": "payment connection timed out"},
        desired_state={"healthy": True},
        resources=[{"id": "node"}],
        memory_scope=MemoryScope(namespace="fixture", partition=name),
        payload={"index_fingerprint": "pinned"},
    )


def candidate():
    return ActionCandidate(
        id="inspect",
        tool="fixture.read",
        plugin_version="1",
        args={"entity": "payment"},
        description="Inspect payment connection endpoint",
        resources=["node"],
        effect="read_only",
        verification="check",
    )


class MockEmbedder:
    def __init__(self):
        self.calls = []
        self.fail = False

    async def embed(self, texts, incident):
        self.calls.append((incident, list(texts)))
        if self.fail:
            raise EmbeddingUnavailable(
                "offline transient",
                code="embedding_unavailable",
                category="transport",
                stage="http",
            )
        return [
            [1.0, float("payment" in text), float("connection" in text)]
            for text in texts
        ]


@unittest.skipUnless(HAS_QDRANT, "install semantic-memory extra")
class SemanticTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "history.sqlite3"
        self.settings = SemanticSettings(dimensions=3, env_file=None)
        self.embedder = MockEmbedder()
        self.store = SemanticStore(self.path, self.settings, embedder=self.embedder)
        self.state = state()
        await self.store.ingest(self.state)

    async def asyncTearDown(self):
        await self.store.aclose()
        self.tmp.cleanup()

    async def fact(self, entity, payload, key=None, snapshot=None):
        observation = Observation(
            resource_id="node",
            kind="diagnostic",
            payload=payload,
            immutable_snapshot=snapshot,
        )
        parsed = ParseResult(
            status="valid",
            parser_version="fixture/1",
            observations=[observation],
            memory_facts=[
                MemoryFact(
                    key=key or entity,
                    resource_id="node",
                    payload={"entity": entity, **payload},
                    evidence_refs=[observation.id],
                )
            ],
        )
        return await self.store.append(
            self.state.incident_id,
            "parse_outcome",
            {"execution_id": "fixture", "parse": parsed.model_dump(mode="json")},
            None,
        )

    async def test_automatic_context_and_cache(self):
        event = await self.fact("payment", {"error": "connection timed out"})
        builder = DefaultContextBuilder(ContextSettings(profile="semantic"), self.store)
        action = candidate()
        request = await builder.build(self.state, [action])
        self.assertEqual(request.candidates, [action])
        self.assertTrue(
            any(
                f["event_ref"] == f"trial:{event.sequence}"
                for f in request.memory.facts
            )
        )
        count = len(self.embedder.calls)
        again = await builder.build(self.state, [candidate()])
        self.assertEqual(len(self.embedder.calls), count)
        self.assertTrue(again.retrieval["cache_hit"])
        self.assertFalse("embedding" in json.dumps(again.memory.model_dump()))

    async def test_restart_reuses_durable_embeddings_without_effects(self):
        await self.fact("payment", {"error": "connection timed out"})
        original = await self.store.retrieve_memory(self.state, [candidate()], None)
        await self.store.aclose()
        self.store = SemanticStore(self.path, self.settings, embedder=self.embedder)
        count = len(self.embedder.calls)
        reopened = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertEqual(len(self.embedder.calls), count)
        self.assertEqual(
            original["retrieval"]["selected"], reopened["retrieval"]["selected"]
        )
        self.assertFalse(
            any(
                e.kind == "execution_started" for e in await self.store.history("trial")
            )
        )

    async def test_trial_isolation_and_scope_rejection(self):
        await self.fact("payment", {"error": "connection timed out"})
        other = state("other")
        await self.store.ingest(other)
        result = await self.store.retrieve_memory(other, [candidate()], None)
        self.assertEqual(result["memory"]["facts"], [])
        changed = self.state.model_copy(
            update={"memory_scope": MemoryScope(namespace="fixture", partition="other")}
        )
        with self.assertRaises(ValueError):
            await self.store.retrieve_memory(changed, [], None)

    async def test_graph_uses_only_admitted_edges(self):
        await self.fact(
            "payment", {"relationships": [["payment", "database", "depends_on"]]}
        )
        await self.fact("database", {"error": "unavailable"})
        await self.fact("unrelated", {"error": "unavailable"})
        result = await self.store.retrieve_memory(self.state, [candidate()], None)
        database = [
            f for f in result["memory"]["facts"] if f["payload"]["entity"] == "database"
        ]
        self.assertTrue(
            any(
                reason["method"] == "graph"
                for f in database
                for reason in f["retrieval_reasons"]
            )
        )
        self.assertFalse(
            any(
                reason["method"] == "graph"
                for f in result["memory"]["facts"]
                if f["payload"]["entity"] == "unrelated"
                for reason in f["retrieval_reasons"]
            )
        )

    async def test_original_freshness_and_conflicts(self):
        await self.fact("payment", {"healthy": False}, key="health", snapshot="pinned")
        await self.fact("payment", {"healthy": True}, key="health")
        result = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertEqual(
            {f["status"] for f in result["memory"]["facts"]}, {"contradicted"}
        )
        self.assertEqual(
            {f["freshness"] for f in result["memory"]["facts"]},
            {"pinned_snapshot", "live_original_timestamp"},
        )
        self.assertTrue(all(f["observed_at"] for f in result["memory"]["facts"]))

    async def test_updated_execution_is_not_stuck_unknown(self):
        c = candidate()
        await self.store.append(
            "trial",
            "execution_started",
            {"execution_id": "run", "candidate": c.model_dump(mode="json")},
            None,
        )
        before = await self.store.retrieve_memory(self.state, [c], None)
        self.assertEqual(before["memory"]["actions"][0]["status"], "unknown")
        await self.store.append(
            "trial",
            "execution_result",
            {
                "execution_id": "run",
                "tool": c.tool,
                "status": "failed",
                "detail": "connection timed out",
                "parse": {
                    "status": "skipped",
                    "parser_version": "fixture/1",
                    "reason": "no output",
                },
            },
            None,
        )
        after = await self.store.retrieve_memory(self.state, [c], None)
        self.assertEqual(after["memory"]["actions"][0]["status"], "failed")
        self.assertEqual(after["memory"]["progress"]["outcomes"], {"failed": 1})

    async def test_transient_failure_and_unknown_reopen_do_not_replay(self):
        await self.fact("payment", {"error": "connection timed out"})
        self.embedder.fail = True
        failed = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertEqual(failed["retrieval"]["status"], "degraded")
        self.assertTrue(failed["memory"]["facts"])
        self.embedder.fail = False
        # Finish any unattempted batches, then verify unknown jobs remain blocked.
        await self.store.retrieve_memory(self.state, [candidate()], None)
        count = len(self.embedder.calls)
        await self.store.aclose()
        self.store = SemanticStore(self.path, self.settings, embedder=self.embedder)
        recovered = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertEqual(len(self.embedder.calls), count)
        self.assertEqual(recovered["retrieval"]["status"], "degraded")

    async def test_recall_and_history_are_not_reindexed(self):
        obs = Observation(
            resource_id="node", kind="history", payload={"answer": "must not index"}
        )
        await self.store.append(
            "trial",
            "parse_outcome",
            {
                "execution_id": "recall",
                "parse": ParseResult(
                    status="valid", parser_version="history/1", observations=[obs]
                ).model_dump(mode="json"),
            },
            None,
        )
        result = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertEqual(result["retrieval"]["indexed_documents"], 0)
        self.assertEqual(self.embedder.calls, [])

    async def test_dynamic_recall_and_expired_intents(self):
        await self.fact("payment", {"error": "connection timed out"})
        plugin = SemanticMemoryPlugin(
            AppContext({"event_store": self.store}, Limits(), Path(self.tmp.name))
        )
        actions = await plugin.recall_candidates(self.state, [candidate()])
        self.assertTrue(actions)
        selected = actions[0]
        self.assertTrue((await plugin.validate(selected, self.state)).allowed)
        result = await plugin.execute(selected, self.state)
        self.assertTrue(result.raw_output["memory"]["facts"])
        self.assertEqual((await plugin.parse(selected, result)).observations, [])
        changed = self.state.model_copy(update={"alert": {"symptom": "different"}})
        await plugin.recall_candidates(changed, [])
        self.assertFalse((await plugin.validate(selected, changed)).allowed)

    async def test_large_segments_budget_and_omissions(self):
        self.store.settings = self.settings.model_copy(
            update={"memory_tokens": 2048, "max_documents": 2, "chunk_tokens": 128}
        )
        await self.fact("payment", {"log": "connection timed out " * 500})
        result = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertLessEqual(result["retrieval"]["estimated_memory_tokens"], 2048)
        self.assertGreater(result["retrieval"]["omitted_count"], 0)
        self.assertTrue(
            any(f["payload"]["incomplete_segment"] for f in result["memory"]["facts"])
        )
        self.assertTrue(result["retrieval"]["omitted_reference"])

    async def test_platform_hints_are_optional_and_dynamic(self):
        await self.store.set_memory_query_hints(
            self.state,
            [MemoryQueryIntent(text="payment endpoint errors", entity="payment")],
        )
        intents = await self.store.memory_query_intents(self.state, [candidate()])
        self.assertTrue(any(v["entity"] == "payment" for v in intents.values()))

    async def test_deterministic_arm_never_embeds(self):
        self.store.settings = self.settings.model_copy(update={"mode": "deterministic"})
        await self.fact("payment", {"error": "connection timed out"})
        result = await self.store.retrieve_memory(self.state, [candidate()], None)
        self.assertTrue(result["memory"]["facts"])
        self.assertEqual(self.embedder.calls, [])

    async def test_received_response_repairs_cache_and_billing_without_api_replay(self):
        text = "payment connection error"
        ledger = Ledger(Path(self.store.settings.ledger), price=100)
        call = ledger.reserve(
            "trial",
            tokens=20,
            token_ceiling=100,
            endpoint="/v1/embeddings",
            model="text-embedding-3-small",
            input_price=20,
        )
        with self.store.lock, self.store.connection:
            self.store.connection.execute(
                "INSERT INTO semantic_pending VALUES(?,?,'unknown')",
                (self.store.cache_key(text), "trial"),
            )
        await self.store.append(
            "trial",
            "embedding_request",
            {
                "request_id": "received",
                "attempt": 1,
                "model": "text-embedding-3-small",
                "dimensions": 3,
                "renderer": "semantic/1",
                "input": [text],
                "call_id": call,
            },
            None,
        )
        await self.store.append(
            "trial",
            "embedding_response",
            {
                "request_id": "received",
                "attempt": 1,
                "response": {
                    "model": "text-embedding-3-small",
                    "data": [{"index": 0, "embedding": [1.0, 0.0, 0.0]}],
                    "usage": {"prompt_tokens": 12, "total_tokens": 12},
                },
            },
            None,
        )
        self.store.recover_embeddings("trial")
        found, degraded = await self.store.vectors([text], "trial")
        self.assertFalse(degraded)
        self.assertEqual(found[text], [1.0, 0.0, 0.0])
        self.assertEqual(self.embedder.calls, [])
        self.assertEqual(ledger.stats()["unknown_calls"], 0)
        self.assertEqual(ledger.stats()["accounted_nanodollars"], 240)

    async def test_unknown_embedding_does_not_veto_fresh_authorized_incident(self):
        self.embedder.fail = True
        await self.store.vectors(["payment"], "trial")
        self.embedder.fail = False
        await self.store.ingest(state("other"))
        found, degraded = await self.store.vectors(["payment"], "other")
        self.assertTrue(found)
        self.assertFalse(degraded)
        self.assertEqual(len(self.embedder.calls), 2)

    async def test_equal_scope_does_not_expose_other_incident_in_history_menu(self):
        await self.fact("payment", {"error": "connection timed out"})
        other = state("other").model_copy(
            update={"memory_scope": self.state.memory_scope}
        )
        await self.store.ingest(other)
        self.assertEqual((await self.store.memory(other, [], 12))["facts"], [])

    async def test_filtered_recall_graph_keeps_strict_entity_boundary(self):
        await self.fact(
            "payment", {"relationships": [["payment", "database", "depends_on"]]}
        )
        await self.fact("database", {"error": "failed"})
        hint = MemoryQueryIntent(text="payment", entity="payment")
        await self.store.set_memory_query_hints(self.state, [hint])
        intents = await self.store.memory_query_intents(self.state, [])
        selected = next(k for k, v in intents.items() if v["entity"] == "payment")
        result = await self.store.search_memory(self.state, selected)
        self.assertTrue(
            all(f["payload"]["entity"] == "payment" for f in result["memory"]["facts"])
        )


class PureMemoryTests(unittest.TestCase):
    def test_queries_follow_incident_without_command_dictionary(self):
        intents = query_intents(state(), [candidate()])
        self.assertTrue(any(q.source == "action" for q in intents.values()))
        self.assertFalse(
            any("wireshark" in q.text or "ps aux" in q.text for q in intents.values())
        )
        self.assertNotEqual(
            set(intents),
            set(
                query_intents(
                    state().model_copy(update={"alert": {"symptom": "disk full"}}),
                    [candidate()],
                )
            ),
        )

    def test_chunks_preserve_unicode_and_bound_estimated_input(self):
        value = {"records": [{"message": "é界 " * 500}]}
        parts = chunks(value, "text-embedding-3-small", 128)
        self.assertGreater(len(parts), 1)
        self.assertTrue(
            all(
                estimate(p, "text-embedding-3-small")["estimated_tokens"] <= 128
                for p in parts
            )
        )
        self.assertIn("é界", "".join(parts))

    def test_only_explicit_graph_edges(self):
        self.assertEqual(relationships({"text": "payment depends on database"}), set())
        self.assertEqual(
            relationships({"records": [{"relationships": [["a", "b", "owns"]]}]}),
            {("a", "b", "owns")},
        )

    def test_endpoint_pricing_and_unknown_reservations(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(Path(tmp) / "ledger.sqlite3", price=100)
            call = ledger.reserve(
                "trial",
                tokens=100,
                token_ceiling=100,
                endpoint="/v1/embeddings",
                model="text-embedding-3-small",
                input_price=20,
            )
            ledger.reconcile(call, {"input_tokens": 50, "output_tokens": 0}, 0.1)
            ledger.reserve("trial", tokens=100, token_ceiling=100)
            self.assertEqual(ledger.stats()["accounted_nanodollars"], 11000)
            self.assertEqual(ledger.stats()["unknown_calls"], 1)
            self.assertEqual(len(ledger.by_endpoint()), 2)

    def test_arm_config_validates_without_importing_sdks(self):
        import builtins

        original_import = builtins.__import__

        def offline_import(name, *args, **kwargs):
            if name.split(".")[0] in {"openai", "qdrant_client", "dotenv", "tiktoken"}:
                raise AssertionError(
                    "optional SDK imported during configuration validation"
                )
            return original_import(name, *args, **kwargs)

        prepared = {"indexes": {"Scenario-1": {"path": "/fixture/index"}}}
        for arm in "BCD":
            with patch.dict(
                DEFAULTS,
                {"provider": "openai", "memory_arm": arm, "semantic_memory": {}},
            ):
                config = trial_config(
                    Path("/fixture/campaign"), prepared, 1, "trial", 42
                )
                with patch("builtins.__import__", offline_import):
                    loaded, _ = validate_config(config)
                self.assertEqual(loaded["context_builder"][1].profile, "semantic")
                self.assertEqual(
                    config.event_store.factory, "dowser.semantic_store:semantic_store"
                )

    def test_retrieval_metrics_and_scenario_bootstrap(self):
        scores = retrieval_metrics({"a": 2, "b": 1}, ["a", "b", "a"])
        self.assertEqual(scores, {"recall_at_24": 1.0, "ndcg_at_24": 1.0})
        paired = clustered_difference(
            {(1, 42): 0, (1, 43): 0, (2, 42): 0},
            {(1, 42): 1, (1, 43): 1, (2, 42): 1},
            iterations=100,
        )
        self.assertEqual(paired["ci95"], [1.0, 1.0])

    def test_campaign_comparison_and_incomplete_campaign_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            campaigns = {}
            scenarios = [*range(1, 40), 102]
            for arm, score in zip("ABCD", (0.1, 0.2, 0.4, 0.7), strict=True):
                directory = Path(tmp) / arm
                (directory / "trials").mkdir(parents=True)
                campaigns[arm] = directory
                config = {
                    "provider": "openai",
                    "model": "gpt-6-luna",
                    "memory_arm": arm,
                    "memory_profile": "large" if arm == "A" else "semantic",
                    "semantic_memory": {
                        "mode": {
                            "A": "legacy",
                            "B": "deterministic",
                            "C": "vector",
                            "D": "graph",
                        }[arm]
                    },
                    "assessments": False,
                }
                manifest = {
                    "configuration": config,
                    "dataset_fingerprint": "fixture",
                    "dataset_revision": "pinned",
                    "code_hashes": {},
                    "dependency_lock_sha256": "locked",
                    "python": "3.13",
                }
                (directory / "manifest.json").write_text(json.dumps(manifest))
                for scenario in scenarios:
                    for seed in (42, 43, 44):
                        (directory / "trials" / f"s{scenario}-{seed}.json").write_text(
                            json.dumps(
                                {
                                    "status": "completed",
                                    "phase": "full",
                                    "scenario": scenario,
                                    "seed": seed,
                                    "score": score,
                                    "category": "completed",
                                }
                            )
                        )
            result = compare(campaigns)
            self.assertEqual(
                result["recommendation"], "D_eligible_after_correctness_gate"
            )
            self.assertEqual(result["paired"]["heldout"]["A_to_D"]["scenarios"], 30)
            (campaigns["D"] / "trials" / "s1-42.json").unlink()
            self.assertFalse(compare(campaigns)["complete"])
            self.assertEqual(compare(campaigns)["recommendation"], "retain_A")


class EmbeddingTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_malformed_response_and_credential_boundaries(self):
        for mode in ("retry", "malformed", "credential", "unlimited"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                settings = SemanticSettings(
                    dimensions=3,
                    embedding_rpm=None if mode == "unlimited" else 1000,
                    embedding_tpm=None if mode == "unlimited" else 10000000,
                    unlimited_account_limits=mode == "unlimited",
                    env_file=None,
                    ledger=str(path / "ledger.sqlite3"),
                )
                ledger = Ledger(path / "ledger.sqlite3", price=100)
                store = SemanticStore(path / "history.sqlite3", settings)
                await store.ingest(state())
                attempts = []

                class RetryError(RuntimeError):
                    status_code = 429
                    response = SimpleNamespace(headers={})

                class Client:
                    def __init__(self, **kwargs):
                        self.embeddings = self

                    async def __aenter__(self):
                        return self

                    async def __aexit__(self, *args):
                        pass

                    async def create(self, **kwargs):
                        attempts.append(kwargs)
                        if mode == "retry" and len(attempts) == 1:
                            raise RetryError("private response must not be logged")
                        vector = (
                            [float("nan"), 0.0, 0.0]
                            if mode == "malformed"
                            else [1.0, 0.0, 0.0]
                        )
                        value = {
                            "model": "text-embedding-3-small",
                            "data": [{"index": 0, "embedding": vector}],
                            "usage": {"prompt_tokens": 10, "total_tokens": 10},
                        }
                        return SimpleNamespace(model_dump=lambda **kw: value)

                try:
                    with (
                        patch.dict(
                            os.environ, {"OPENAI_API_KEY": "offline-transport-key"}
                        ),
                        patch("openai.AsyncOpenAI", Client),
                        patch("dowser.semantic_store.asyncio.sleep", AsyncMock()),
                    ):
                        text = (
                            "offline-transport-key"
                            if mode == "credential"
                            else "payment connection error"
                        )
                        if mode in {"retry", "unlimited"}:
                            await store.embedder.embed([text], "trial")
                        else:
                            with self.assertRaises(DiagnosticError):
                                await store.embedder.embed([text], "trial")
                    self.assertEqual(
                        ledger.stats()["calls"],
                        {"retry": 2, "malformed": 1, "credential": 0, "unlimited": 1}[
                            mode
                        ],
                    )
                    self.assertEqual(
                        ledger.stats()["unknown_calls"], int(mode == "retry")
                    )
                    if mode == "retry":
                        self.assertEqual(
                            ledger.diagnostics()[0]["failure"]["http_status"], 429
                        )
                    history = json.dumps(
                        [
                            e.model_dump(mode="json")
                            for e in await store.history("trial")
                        ]
                    )
                    self.assertNotIn("offline-transport-key", history)
                    self.assertNotIn("private response must not be logged", history)
                finally:
                    await store.aclose()

    async def test_sdk_usage_audit_and_accounting_without_paid_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            Ledger(path / "ledger.sqlite3", price=100)
            settings = SemanticSettings(
                dimensions=3,
                embedding_rpm=1000,
                embedding_tpm=100000,
                env_file=None,
                ledger=str(path / "ledger.sqlite3"),
            )
            store = SemanticStore(path / "history.sqlite3", settings)
            await store.ingest(state())

            class Client:
                def __init__(self, **kwargs):
                    self.embeddings = self
                    assert kwargs["max_retries"] == 0

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                async def create(self, **kwargs):
                    value = {
                        "model": "text-embedding-3-small",
                        "data": [
                            {"index": i, "embedding": [1.0, 0.0, 0.0]}
                            for i, _ in enumerate(kwargs["input"])
                        ],
                        "usage": {"prompt_tokens": 10, "total_tokens": 10},
                    }
                    return SimpleNamespace(model_dump=lambda **kw: value)

            try:
                with (
                    patch.dict(os.environ, {"OPENAI_API_KEY": "offline-secret-value"}),
                    patch("openai.AsyncOpenAI", Client),
                ):
                    vectors = await OpenAIEmbedder(settings, store, path).embed(
                        ["payment connection failure"], "trial"
                    )
                self.assertEqual(vectors, [[1.0, 0.0, 0.0]])
                self.assertEqual(
                    Ledger(path / "ledger.sqlite3").stats()["accounted_nanodollars"],
                    200,
                )
                history = await store.history("trial")
                self.assertTrue(any(e.kind == "embedding_response" for e in history))
                self.assertNotIn(
                    "offline-secret-value",
                    json.dumps([e.model_dump(mode="json") for e in history]),
                )
            finally:
                await store.aclose()


@unittest.skipUnless(HAS_QDRANT, "install semantic-memory extra")
class BenchmarkMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_arms_through_real_loop_and_resume_without_calls(self):
        from test_decisions_history import response
        from test_itbench_aa import ENTITY, fixture

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "dataset" / "sre" / "Scenario-8"
            fixture(source)
            meta = create_index(source, root / "index.sqlite3", 8)
            prepared = {
                "dataset": str(root / "dataset"),
                "indexes": {
                    "Scenario-8": {**meta, "path": str(root / "index.sqlite3")}
                },
                "fingerprint": "fixture",
            }
            for arm in "ABCD":
                with self.subTest(arm=arm):
                    campaign = root / arm
                    campaign.mkdir()
                    Ledger(campaign / "spending.sqlite3", price=100)
                    requests, embedding_calls = [], []
                    recalled = False

                    async def decide(**payload):
                        nonlocal recalled
                        request = decode_decision_input(json.loads(payload["input"]))
                        requests.append(request)
                        working = max(
                            (
                                o["payload"]
                                for o in request["state"]["observations"]
                                if o["kind"] == "investigation"
                            ),
                            key=lambda w: w["revision"],
                        )
                        operation = (
                            "focus"
                            if not working["focus"]
                            else "inspect"
                            if not working["evidence"]["records"]
                            else "memory.search"
                            if arm != "A" and not recalled
                            else "nominate"
                            if not working["nominations"]
                            else "submit"
                        )
                        options = [
                            c
                            for c in request["candidates"]
                            if (
                                c["tool"] == operation
                                if operation == "memory.search"
                                else c["args"].get("operation") == operation
                            )
                            and (
                                operation in {"submit", "memory.search"}
                                or c["args"].get("entity") == ENTITY
                            )
                            and (
                                operation != "inspect"
                                or c["args"].get("kind") == "configuration"
                            )
                            and (
                                operation != "nominate"
                                or c["args"].get("reason") == "configuration"
                            )
                        ]
                        choice = options[0]
                        if operation == "memory.search":
                            recalled = True
                        return response(
                            payload,
                            lambda q: (
                                "action:" + choice["id"]
                                if any(
                                    c["value"] == "action:" + choice["id"]
                                    for c in q.get("choices", [])
                                )
                                else "wait"
                            ),
                        )

                    async def embed(**payload):
                        embedding_calls.append(payload)
                        value = {
                            "model": "text-embedding-3-small",
                            "data": [
                                {"index": i, "embedding": [1.0] * payload["dimensions"]}
                                for i, _ in enumerate(payload["input"])
                            ],
                            "usage": {"prompt_tokens": 10, "total_tokens": 10},
                        }
                        return SimpleNamespace(model_dump=lambda **kw: value)

                    class Client:
                        def __init__(self, **kwargs):
                            self.decisions = SimpleNamespace(create=decide)
                            self.embeddings = SimpleNamespace(create=embed)

                        async def __aenter__(self):
                            return self

                        async def __aexit__(self, *args):
                            pass

                    defaults = {
                        "provider": "openai",
                        "memory_arm": arm,
                        "semantic_memory": {
                            "dimensions": 3,
                            "embedding_rpm": 1000,
                            "embedding_tpm": 10000000,
                        },
                        "openai": {
                            "requests_per_minute": 1000,
                            "tokens_per_minute": 10000000,
                        },
                    }
                    with (
                        patch.dict(DEFAULTS, defaults),
                        patch.dict(
                            os.environ, {"OPENAI_API_KEY": "offline-benchmark-key"}
                        ),
                        patch("openai.AsyncOpenAI", Client),
                    ):
                        row = await run_trial(
                            campaign, prepared, 8, "pilot-s8", 42, "pilot", root
                        )
                        calls = (len(requests), len(embedding_calls))
                        resumed = await run_trial(
                            campaign, prepared, 8, "pilot-s8", 42, "pilot", root
                        )
                    self.assertEqual(row["category"], "completed", row.get("failure"))
                    self.assertEqual(row["score"], 1)
                    self.assertEqual(row, resumed)
                    self.assertEqual(calls, (len(requests), len(embedding_calls)))
                    self.assertEqual(bool(embedding_calls), arm in "CD")
                    self.assertTrue(
                        all(
                            any(
                                c["args"].get("operation") == "browse"
                                for c in r["candidates"]
                            )
                            for r in requests
                        )
                    )
                    self.assertNotIn("ground_truth", json.dumps(requests))
                    self.assertNotIn("MUST_NEVER_ENTER_MODEL", json.dumps(requests))
                    if arm != "A":
                        self.assertTrue(requests[-1]["memory"]["facts"])
                        store = SemanticStore(
                            campaign / "history.sqlite3",
                            SemanticSettings(mode="deterministic"),
                        )
                        try:
                            events = await store.history("pilot-s8")
                            fact = next(
                                e
                                for e in events
                                if e.kind == "parse_outcome"
                                and e.payload["parse"].get("memory_facts")
                            )
                            request = next(
                                e
                                for e in events
                                if e.kind == "provider_request"
                                and e.sequence > fact.sequence
                            )
                            labels = [
                                {
                                    "request_ref": f"pilot-s8:{request.sequence}",
                                    "relevant": {f"pilot-s8:{fact.sequence}": 2},
                                }
                            ]
                            gate = evaluate_retrieval(
                                campaign / "history.sqlite3", labels
                            )
                            self.assertTrue(gate["gate_passed"])
                            labels[0]["relevant"] = {
                                f"pilot-s8:{request.sequence + 1}": 1
                            }
                            with self.assertRaises(ValueError):
                                evaluate_retrieval(campaign / "history.sqlite3", labels)
                        finally:
                            await store.aclose()

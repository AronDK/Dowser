"""Offline Decisions, retrieval, cumulative context and mirror boundary tests."""

import asyncio
import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dowser.assessments import AssessmentQuestion, AssessmentRequest
from dowser.bench_ledger import Ledger
from dowser.catalogue import decode_catalogue, decode_decision_input
from dowser.contracts import AppContext
from dowser.core import (
    ContextSettings,
    DefaultContextBuilder,
    DefaultPolicy,
    PolicySettings,
)
from dowser.memory import action_identity
from dowser.models import (
    ActionCandidate,
    ActionIdentity,
    DecisionRequest,
    IncidentState,
    Limits,
    MemoryFact,
    MemoryScope,
    Observation,
    ParseResult,
    now,
)
from dowser.rate_limit import RateLimiter
from dowser.store import SQLiteStore
from plugins.openai_decisions import (
    DecisionsError,
    OpenAIDecisionsProvider,
    OpenAISettings,
)


def state(name="trial"):
    return IncidentState(
        incident_id=name,
        alert={"symptom": "database unavailable"},
        desired_state={},
        resources=[{"id": "node"}],
        memory_scope=MemoryScope(namespace="fixture", partition=name),
    )


def candidate(name="read"):
    return ActionCandidate(
        id=name,
        tool="fixture.read",
        plugin_version="1",
        args={"entity": "database"},
        description="Inspect database",
        resources=["node"],
        effect="read_only",
        verification="check",
    )


def response(payload, choose=None):
    answers = []
    for q in payload["questions"]:
        if q["type"] == "predicate":
            answers.append({"type": "predicate", "name": q["name"], "probability": 0.7})
        elif q["type"] == "score":
            answers.append(
                {
                    "type": "score",
                    "name": q["name"],
                    "score": 0.7,
                    "confidence": 0.8,
                    "probabilities": [
                        {
                            "value": i,
                            "label": level["label"],
                            "probability": 0.3 if i == 0 else 0.7,
                        }
                        for i, level in enumerate(q["levels"])
                    ],
                }
            )
        else:
            choice = choose(q) if choose else q["choices"][0]["value"]
            answers.append(
                {
                    "type": "choice",
                    "name": q["name"],
                    "choice": choice,
                    "confidence": 0.2,
                    "probabilities": [
                        {
                            "value": c["value"],
                            "probability": float(c["value"] == choice),
                        }
                        for c in q["choices"]
                    ],
                }
            )
    return {
        "model": "gpt-6-luna",
        "answers": answers,
        "usage": {"input_tokens": 100, "output_tokens": 0},
    }


class FakeClient:
    def __init__(self, handle):
        self.decisions = SimpleNamespace(create=handle)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class DecisionsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = SQLiteStore(
            self.root / "history.sqlite3", self.root / "audit.jsonl"
        )
        self.state = state()
        await self.store.ingest(self.state)
        self.request = DecisionRequest(
            incident_id="trial", state=self.state, candidates=[candidate()]
        )
        self.context = AppContext({"event_store": self.store}, Limits(), self.root)
        self.env = patch.dict(os.environ, {"OPENAI_API_KEY": "private-test-credential"})
        self.env.start()
        self.calls = []

    async def asyncTearDown(self):
        self.env.stop()
        await self.store.aclose()
        self.tmp.cleanup()

    def provider(self, handle=None, **settings):
        async def invoke(**payload):
            self.calls.append(payload)
            return await handle(payload) if handle else response(payload)

        return OpenAIDecisionsProvider(
            OpenAISettings(
                requests_per_minute=1000, tokens_per_minute=100000000, **settings
            ),
            self.context,
            client_factory=lambda **kwargs: FakeClient(invoke),
        )

    async def test_native_choice_audit_and_no_credential(self):
        result = await self.provider().decide(self.request)
        self.assertEqual(result.candidate_id, "read")
        self.assertEqual(
            result.score_metadata["grouping_stages"][0]["native_answers"][0][
                "confidence"
            ],
            0.2,
        )
        self.assertEqual(
            json.loads(self.calls[0]["input"])["candidates"][0]["id"], "read"
        )
        history = await self.store.history("trial")
        self.assertIn("provider_api_request", [e.kind for e in history])
        self.assertIn("provider_api_response", [e.kind for e in history])
        self.assertNotIn(
            "private-test-credential", (self.root / "audit.jsonl").read_text()
        )
        self.assertFalse(
            result.score_metadata["grouping_stages"][0]["estimate"]["exact"]
        )

    async def test_catalogue_defaults_preserve_all_fields_and_json_types(self):
        from dowser.catalogue import encode_catalogue

        values = [candidate(str(i)).model_dump(mode="json") for i in range(20)]
        values[0]["args"]["flag"] = True
        for value in values[1:]:
            value["args"]["flag"] = 1
        encoded = encode_catalogue(values)
        self.assertEqual(decode_catalogue(encoded), values)
        self.assertIs(decode_catalogue(encoded)[0]["args"]["flag"], True)
        self.assertLess(len(json.dumps(encoded)), len(json.dumps(values)))

    async def test_history_defaults_and_string_references_round_trip_failures(self):
        self.request.candidates = [candidate(str(i)) for i in range(20)]
        for c in self.request.candidates:
            c.args["entity"] = "observed-namespace/Deployment/long-observed-entity"
        self.request.action_history = {
            c.id: {
                "lookup_status": "ok",
                "record_status": "no_previous_record",
                "usage_count": 0,
            }
            for c in self.request.candidates
        }
        self.request.action_history["3"] = {
            "lookup_status": "failed",
            "error_type": "SQLiteError",
        }
        encoded = json.loads(self.provider().input(self.request))
        decoded = decode_decision_input(encoded)
        self.assertEqual(
            decoded["candidates"],
            [c.model_dump(mode="json") for c in self.request.candidates],
        )
        self.assertEqual(decoded["action_history"], self.request.action_history)
        self.assertNotIn("record_status", decoded["action_history"]["3"])
        self.assertTrue(encoded["candidate_string_table"])
        self.assertIn("action_history_default", encoded)

    async def test_description_templates_preserve_literals_and_argument_references(
        self,
    ):
        from dowser.catalogue import encode_catalogue

        values = [candidate(str(i)).model_dump(mode="json") for i in range(20)]
        for i, value in enumerate(values):
            value["args"].update(
                entity=f"namespace/Deployment/entity-{i}",
                kind="configuration",
                reason="dependency failure",
            )
            value["description"] = (
                f"Inspect configuration for {value['args']['entity']}; dependency failure; {value['args']['entity']}"
            )
        encoded = encode_catalogue(values)
        self.assertEqual(decode_catalogue(encoded), values)
        self.assertTrue(encoded["candidate_description_templates"])
        self.assertTrue(
            all(type(c["description"]) is int for c in encoded["candidates"])
        )

    async def test_rate_admission_splits_questions_without_omitting_candidates(self):
        self.request.candidates = [candidate(f"option-{i:04}") for i in range(1600)]
        provider = self.provider()
        shared = provider.input(self.request)
        from dowser.tokens import estimate

        base = (
            estimate({"model": "gpt-6-luna", "input": shared, "questions": []})[
                "estimated_tokens"
            ]
            + 64
        )
        questions = provider.batches(self.request.candidates)[0]
        provider.settings.tokens_per_minute = base + sum(
            estimate(q)["estimated_tokens"] for q in questions[:2]
        )
        packed = provider.batches(self.request.candidates, shared=shared)
        self.assertTrue(all(len(batch) <= 2 for batch in packed))
        offered = [
            c["value"]
            for batch in packed
            for q in batch
            for c in q["choices"]
            if c["value"] != "wait"
        ]
        self.assertEqual(len(offered), 1600)
        self.assertEqual(len(set(offered)), 1600)

    async def test_every_candidate_in_grouped_selection_and_final_choice(self):
        self.request.candidates = [candidate(f"option-{i:04}") for i in range(1600)]
        result = await self.provider().decide(self.request)
        first_stage = self.calls[:2]
        participating = [
            c["value"].removeprefix("action:")
            for p in first_stage
            for q in p["questions"]
            for c in q["choices"]
            if c["value"] != "wait"
        ]
        self.assertEqual(set(participating), {c.id for c in self.request.candidates})
        self.assertEqual(len(participating), 1600)
        self.assertEqual([len(p["questions"]) for p in first_stage], [6, 1])
        self.assertTrue(
            all(len(q["choices"]) <= 255 for p in self.calls for q in p["questions"])
        )
        self.assertEqual(len(self.calls[-1]["questions"][0]["choices"]), 8)
        self.assertEqual(len({p["input"] for p in self.calls}), 1)
        self.assertEqual(result.candidate_id, "option-0000")

    async def test_all_groups_wait_and_winner_wait(self):
        self.request.candidates = [candidate(str(i)) for i in range(300)]

        async def wait(payload):
            return response(payload, lambda q: "wait")

        result = await self.provider(wait).decide(self.request)
        self.assertEqual(result.operation, "wait")
        self.assertEqual(len(self.calls), 1)
        self.calls.clear()

        async def final_wait(payload):
            return response(
                payload,
                lambda q: (
                    "wait"
                    if q["name"].startswith("stage_1")
                    else q["choices"][0]["value"]
                ),
            )

        self.assertEqual(
            (await self.provider(final_wait).decide(self.request)).operation, "wait"
        )
        self.assertEqual(len(self.calls), 2)

    async def test_refusals_unknown_choices_malformed_and_model_mismatch_not_retried(
        self,
    ):
        for mutate in (
            lambda v: v["answers"][0].update(type="refusal"),
            lambda v: v["answers"][0].update(choice="unknown"),
            lambda v: v["answers"][0].update(confidence=float("nan")),
            lambda v: v.update(model="other-model"),
            lambda v: v.update(answers=[]),
        ):
            self.calls.clear()

            async def bad(payload):
                value = response(payload)
                mutate(value)
                return value

            with self.assertRaises(DecisionsError):
                await self.provider(bad).decide(self.request)
            self.assertEqual(len(self.calls), 1)

    async def test_retries_accounting_known_usage_even_for_bad_answers(self):
        ledger = Ledger(self.root / "ledger.sqlite3", price=100)

        class Accounting:
            def start(inner, request_id, attempt, tokens):
                return ledger.reserve(
                    "trial",
                    attempt=attempt,
                    tokens=tokens,
                    token_ceiling=1050000,
                    provider="openai",
                    model="gpt-6-luna",
                    long_context_threshold=272000,
                    long_context_multiplier=2,
                )

            def success(inner, call_id, usage, latency):
                ledger.reconcile(call_id, usage, latency)

            def failure(inner, call_id, error, latency):
                ledger.failure(call_id, error, latency)

        async def retry(payload):
            if len(self.calls) == 1:
                raise ConnectionError("private-test-credential")
            return response(payload)

        provider = self.provider(retry, retry_initial_seconds=0.001)
        provider.accounting = Accounting()
        await provider.decide(self.request)
        self.assertEqual(ledger.stats()["unknown_calls"], 1)
        self.assertEqual(ledger.stats()["calls"], 2)
        self.calls.clear()

        async def malformed(payload):
            value = response(payload)
            value["answers"] = []
            return value

        provider = self.provider(malformed)
        provider.accounting = Accounting()
        with self.assertRaises(DecisionsError):
            await provider.decide(self.request)
        self.assertEqual(ledger.stats()["unknown_calls"], 1)
        self.assertEqual(ledger.stats()["input_tokens"], 200)

    async def test_account_limits_required_and_attempt_cancellation(self):
        provider = OpenAIDecisionsProvider(OpenAISettings(), self.context)
        with self.assertRaises(DecisionsError) as error:
            await provider.decide(self.request)
        self.assertEqual(error.exception.detail.code, "missing_account_limits")

        async def delayed(payload):
            await asyncio.sleep(10)

        with self.assertRaises(DecisionsError):
            await self.provider(
                delayed, attempt_timeout_seconds=0.01, max_retries=0
            ).decide(self.request)
        task = asyncio.create_task(self.provider(delayed).decide(self.request))
        await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_predicate_choice_score_assessments(self):
        request = AssessmentRequest(
            incident_id="trial",
            state={"observed": True},
            questions={
                "predicate": AssessmentQuestion(type="noul", instructions="Relevant?"),
                "choice": AssessmentQuestion(
                    type="choice",
                    instructions="Which?",
                    criteria={"a": "first", "b": "second"},
                ),
                "score": AssessmentQuestion(
                    type="score", instructions="Support?", criteria=["low", "high"]
                ),
            },
        )
        result = await self.provider().assess(request)
        self.assertEqual(result.answers["predicate"].noul, 0.7)
        self.assertEqual(result.answers["choice"].choice, "a")
        self.assertEqual(result.answers["score"].score, 0.7)
        self.assertTrue(result.score_metadata["interpretation"])

    async def test_official_sdk_request_with_mock_transport(self):
        import httpx2
        from openai import AsyncOpenAI

        def handle(request):
            payload = json.loads(request.content)
            self.calls.append(payload)
            self.assertEqual(request.url.path, "/v1/decisions")
            return httpx2.Response(200, json=response(payload))

        provider = OpenAIDecisionsProvider(
            OpenAISettings(requests_per_minute=100, tokens_per_minute=1000000),
            self.context,
            client_factory=lambda **kw: AsyncOpenAI(
                **kw,
                http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handle)),
            ),
        )
        from dowser.runtime import bounded_call

        self.assertEqual(
            (await bounded_call(provider.decide, self.request, seconds=5)).candidate_id,
            "read",
        )


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = SQLiteStore(
            self.root / "history.sqlite3", self.root / "audit.jsonl"
        )
        self.state = state()
        await self.store.ingest(self.state)
        self.candidate = candidate()

    async def asyncTearDown(self):
        await self.store.aclose()
        self.tmp.cleanup()

    async def action(self, execution, outcome=None, version="v1"):
        c = self.candidate
        identity = ActionIdentity(key=action_identity(c).key, evidence_version=version)
        await self.store.append(
            "trial",
            "execution_started",
            {
                "execution_id": execution,
                "candidate": c.model_dump(mode="json"),
                "identity": identity.model_dump(),
            },
        )
        ref = f"trial/{execution}/raw"
        await self.store.append(
            "trial",
            "raw_output",
            {"execution_id": execution, "raw_output_refs": [ref]},
            {ref: {"database": "connection refused"}},
        )
        if outcome:
            await self.store.append(
                "trial",
                "execution_result",
                {
                    "execution_id": execution,
                    "status": outcome,
                    "parse": {"status": "skipped"},
                    "detail": "connection refused",
                },
            )
        return identity

    async def test_new_failed_unknown_rejected_and_older_versions(self):
        identity = action_identity(self.candidate)
        lookup = await self.store.action_history(self.state, {"new": identity})
        self.assertEqual(lookup["new"]["record_status"], "no_previous_record")
        await self.action("failed", "failed")
        await self.action("unknown")
        await self.store.append(
            "trial",
            "validation_outcome",
            {
                "allowed": False,
                "reason": "precondition failed",
                "candidate": self.candidate.model_dump(mode="json"),
                "identity": identity.model_dump(),
            },
        )
        history = (
            await self.store.action_history(self.state, {"renamed-uuid": identity})
        )["renamed-uuid"]
        self.assertEqual(history["usage_count"], 2)
        self.assertEqual(history["rejection_count"], 1)
        self.assertEqual(history["outcomes"]["unknown"], 1)
        self.assertTrue(any(e["older_evidence_version"] for e in history["entries"]))
        self.assertTrue(history["latest_saved_result"])
        self.assertEqual(history["entries"][0]["reason"], "precondition failed")

    async def test_filters_pagination_artifact_and_trial_isolation(self):
        await self.action("failed", "failed")
        await self.action("unknown")
        result = await self.store.query_history(
            self.state,
            {
                "entity": "database",
                "text": "database",
                "tool": "fixture.read",
                "limit": 1,
            },
        )
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["next_offset"], 1)
        next_page = await self.store.query_history(
            self.state, {"category": "action", "offset": 1, "limit": 1}
        )
        self.assertEqual(next_page["entries"][0]["status"], "failed")
        self.assertEqual(
            (await self.store.query_history(self.state, {"outcome": "failed"}))[
                "total"
            ],
            1,
        )
        self.assertEqual(
            (await self.store.query_history(self.state, {"before": "2000-01-01"}))[
                "total"
            ],
            0,
        )
        self.assertIn(
            "trial/failed/raw",
            (
                await self.store.query_history(
                    self.state, {"artifact_ref": "trial/failed/raw"}
                )
            )["artifacts"],
        )
        other = state("other")
        await self.store.ingest(other)
        self.assertEqual((await self.store.query_history(other, {}))["total"], 0)
        with self.assertRaises(ValueError):
            await self.store.query_history(other, {"artifact_ref": "trial/failed/raw"})
        with self.assertRaises(ValueError):
            await self.store.query_history(self.state, {"sql": "SELECT * FROM events"})
        with self.assertRaises(ValueError):
            await self.store.query_history(
                self.state.model_copy(update={"memory_scope": other.memory_scope}), {}
            )

    async def test_hundreds_of_facts_contradictions_unchanged_dedup_and_overflow(self):
        for i in range(300):
            await self.store.append(
                "trial",
                "parse_outcome",
                {
                    "parse": ParseResult(
                        status="valid",
                        parser_version="fixture",
                        memory_facts=[
                            MemoryFact(
                                key=f"fact-{i}",
                                resource_id="node",
                                payload={
                                    "entity": "database",
                                    "kind": "logs",
                                    "finding": i,
                                },
                            )
                        ],
                    ).model_dump(mode="json")
                },
            )
        for value in ("first", "second", "first"):
            await self.store.append(
                "trial",
                "parse_outcome",
                {
                    "parse": ParseResult(
                        status="valid",
                        parser_version="fixture",
                        memory_facts=[
                            MemoryFact(
                                key="contradiction",
                                resource_id="node",
                                payload={
                                    "entity": "database",
                                    "kind": "logs",
                                    "finding": value,
                                },
                            )
                        ],
                    ).model_dump(mode="json")
                },
            )
        request = await DefaultContextBuilder(
            ContextSettings(profile="large"), self.store
        ).build(self.state, [self.candidate])
        self.assertEqual(len(request.memory.facts), 302)
        self.assertEqual(request.memory.progress["contradiction_count"], 1)
        self.assertEqual(request.memory.progress["omitted_facts"], 0)
        limited = await DefaultContextBuilder(
            ContextSettings(profile="large", input_tokens=2048), self.store
        ).build(self.state, [self.candidate])
        self.assertGreater(limited.memory.progress["omitted_facts"], 0)
        self.assertEqual(limited.candidates, request.candidates)
        self.assertTrue(limited.retrieval["omitted_fact_queries"])
        self.assertEqual(
            (
                await self.store.query_history(
                    self.state, {"category": "fact", "evidence_kind": "logs"}
                )
            )["total"],
            303,
        )

    async def test_mirror_repair_torn_line_failure_and_integrity(self):
        await self.action("unknown")
        path = self.root / "audit.jsonl"
        lines = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(lines[:-1]) + b'{"torn":')
        repaired = self.store.repair_journal()
        self.assertEqual(repaired["recovered"], 1)
        self.assertEqual(path.read_bytes(), b"".join(lines))
        with (
            patch.object(
                self.store,
                "journal_artifact",
                side_effect=OSError("mirror unavailable"),
            ),
            self.assertWarns(RuntimeWarning),
        ):
            await self.store.append("trial", "large", {"data": "x" * 70000})
        self.assertTrue(self.store.journal_error)
        self.assertEqual(self.store.repair_journal()["recovered"], 1)
        self.assertEqual(len((await self.store.inspect("trial"))["executions"]), 1)
        record = json.loads(path.read_text().splitlines()[0])
        record["kind"] = "corrupted"
        corrupted = (
            json.dumps(record).encode()
            + b"\n"
            + b"".join(path.read_bytes().splitlines(keepends=True)[1:])
        )
        path.write_bytes(corrupted)
        with self.assertRaises(ValueError):
            self.store.repair_journal()

    async def test_export_old_database_and_long_context_price(self):
        exported = self.store.repair_journal(self.root / "export.jsonl")
        self.assertEqual(exported["sequence"], 1)
        ledger = Ledger(self.root / "spend.sqlite3", price=100)
        cid = ledger.reserve(
            "trial",
            tokens=300000,
            token_ceiling=1050000,
            provider="openai",
            model="gpt-6-luna",
            long_context_threshold=272000,
            long_context_multiplier=2,
        )
        self.assertEqual(ledger.stats()["accounted_nanodollars"], 60000000)
        ledger.reconcile(cid, {"input_tokens": 280000, "output_tokens": 0}, 0.1)
        self.assertEqual(Ledger(ledger.path).stats()["accounted_nanodollars"], 56000000)

    async def test_repeat_policy_and_snapshot_freshness(self):
        old = Observation(
            resource_id="node",
            kind="probe",
            payload={},
            observed_at=now() - timedelta(hours=1),
        )
        s = self.state.model_copy(update={"observations": [old]})
        c = self.candidate.model_copy(update={"required_observation_ids": [old.id]})
        policy = DefaultPolicy(
            PolicySettings(), AppContext({}, Limits(identical_attempts=1), self.root)
        )
        budget = {"seconds_remaining": 100, "changes": 0, "attempts": 100}
        self.assertFalse((await policy.validate(c, s, budget)).allowed)
        self.assertTrue((await policy.validate(self.candidate, s, budget)).allowed)
        old.immutable_snapshot = "snapshot"
        s.payload["index_fingerprint"] = "snapshot"
        s.memory_scope = MemoryScope(namespace="itbench-aa", partition=s.incident_id)
        s.resources[0].platform = "itbench-aa"
        self.assertTrue((await policy.validate(c, s, budget)).allowed)
        s.payload["index_fingerprint"] = "different"
        self.assertFalse((await policy.validate(c, s, budget)).allowed)

    async def test_history_tool_failure_is_a_saved_failed_read(self):
        from plugins.history import HistoryPlugin

        plugin = HistoryPlugin(
            AppContext({"event_store": self.store}, Limits(), self.root)
        )
        c = (await plugin.candidates(self.state))[0]
        with patch.object(
            self.store, "query_history", side_effect=ValueError("lookup failed")
        ):
            result = await plugin.execute(c, self.state)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.raw_output["result"]["lookup_status"], "failed")
        self.assertNotIn("record_status", result.raw_output["result"])

    async def test_choice_projection_retains_waits_without_copying_score_matrices(self):
        await self.store.append(
            "trial",
            "provider_result",
            {
                "request_id": "fixture",
                "result": {
                    "operation": "wait",
                    "candidate_id": None,
                    "wait_seconds": 1,
                    "reason": "deliberation",
                    "score_metadata": {
                        "provider": "openai",
                        "model": "gpt-6-luna",
                        "matrix": "x" * 100000,
                    },
                },
            },
        )
        await self.store.append(
            "trial", "wait_started", {"reason": "deliberation", "seconds": 1}
        )
        choices = await self.store.recent_choices(self.state, 100)
        self.assertEqual(len(choices), 2)
        self.assertTrue(all(c["operation"] == "wait" for c in choices))
        self.assertNotIn("matrix", json.dumps(choices))
        self.assertEqual(
            (await self.store.query_history(self.state, {"category": "choice"}))[
                "total"
            ],
            2,
        )

    async def test_distinct_contradictions_in_one_parse_are_queryable(self):
        await self.store.append(
            "trial",
            "parse_outcome",
            {
                "parse": ParseResult(
                    status="valid",
                    parser_version="fixture",
                    memory_facts=[
                        MemoryFact(
                            key="same-key",
                            resource_id="node",
                            payload={"finding": value},
                        )
                        for value in ("first", "second")
                    ],
                ).model_dump(mode="json")
            },
        )
        queried = await self.store.query_history(self.state, {"category": "fact"})
        self.assertEqual(queried["total"], 2)
        memory = await self.store.memory(self.state, [], None)
        self.assertEqual(len(memory["facts"]), 2)
        self.assertTrue(all(f["status"] == "contradicted" for f in memory["facts"]))

    async def test_minute_rate_admission(self):
        clock = [0.0]

        async def sleep(delay):
            clock[0] += delay

        limiter = RateLimiter(
            2, 1000, clock=lambda: clock[0], sleeper=sleep, window_seconds=60
        )
        await limiter.acquire(500)
        await limiter.acquire(500)
        third = await limiter.acquire(500)
        self.assertEqual(third.waited, 60)


class NavigationTests(unittest.IsolatedAsyncioTestCase):
    async def test_openai_loop_repeats_fresh_reads_waits_and_keeps_browser(self):
        from test_itbench_aa import ENTITY, fixture

        from dowser.bench import DEFAULTS, trial_config
        from dowser.bench_data import create_index
        from dowser.config import Application
        from dowser.models import RawIncident

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root / "source")
            create_index(root / "source", root / "index.sqlite3", 8)
            account = {
                "requests_per_minute": 1000,
                "tokens_per_minute": 10000000,
                "wait_seconds": 0.001,
            }
            with patch.dict(DEFAULTS, {"provider": "openai", "openai": account}):
                config = trial_config(
                    root / "campaign",
                    {"indexes": {"Scenario-8": {"path": str(root / "index.sqlite3")}}},
                    8,
                    "repeat",
                    42,
                )
            operations = [
                "focus",
                "inspect",
                "wait",
                "browse",
                "focus",
                "inspect",
                "recall",
                "nominate",
                "submit",
            ]
            requests = []

            async def handle(**payload):
                request = decode_decision_input(json.loads(payload["input"]))
                requests.append(request)
                operation = operations[len(requests) - 1]
                self.assertTrue(
                    any(
                        c["args"].get("operation") == "browse"
                        for c in request["candidates"]
                    )
                )
                if operation == "wait":
                    return response(payload, lambda q: "wait")
                choice = next(
                    c
                    for c in request["candidates"]
                    if c["args"].get("operation") == operation
                    and (
                        operation in {"submit", "browse"}
                        or c["args"].get("entity") == ENTITY
                    )
                    and (
                        operation not in {"inspect", "recall"}
                        or c["args"].get("kind") == "configuration"
                    )
                    and (
                        operation != "nominate"
                        or c["args"].get("reason") == "configuration"
                    )
                )
                if operation == "inspect":
                    history = request["action_history"][choice["id"]]
                    self.assertEqual(history["lookup_status"], "ok")
                    self.assertEqual(
                        history["usage_count"], 0 if len(requests) == 2 else 1
                    )
                return response(payload, lambda q: "action:" + choice["id"])

            with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-credential"}):
                async with Application(config, root) as app:
                    incident = await app.services["normalizer"].normalize(
                        RawIncident(
                            source_id="itbench-aa",
                            event_id="repeat",
                            payload={"scenario": 8, "trial": "repeat"},
                        )
                    )
                    incident.observations[0].observed_at = now() - timedelta(hours=1)
                    app.services["decision_provider"].provider.client_factory = (
                        lambda **kw: FakeClient(handle)
                    )
                    plugin = app.services["tool_registry"].plugins[0]
                    with patch.object(
                        plugin.index, "page", wraps=plugin.index.page
                    ) as pages:
                        result = await app.services["incident_loop"].run(incident)
                        self.assertEqual(pages.call_count, 2)
                    self.assertEqual(result.outcome, "resolved")
                    history = await app.services["event_store"].history("repeat")
                    self.assertEqual(
                        len([e for e in history if e.kind == "wait_started"]), 1
                    )
                    self.assertEqual(
                        len([e for e in history if e.kind == "execution_started"]), 8
                    )
                    self.assertTrue(requests[-1]["retrieval"]["recent_model_choices"])

    async def test_lookup_failures_remain_explicit_and_do_not_veto_authorized_actions(
        self,
    ):
        from test_itbench_aa import ENTITY, fixture

        from dowser.bench import DEFAULTS, trial_config
        from dowser.bench_data import create_index
        from dowser.config import Application
        from dowser.models import RawIncident

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root / "source")
            create_index(root / "source", root / "index.sqlite3", 8)
            with patch.dict(
                DEFAULTS,
                {
                    "provider": "openai",
                    "openai": {
                        "requests_per_minute": 1000,
                        "tokens_per_minute": 10000000,
                    },
                },
            ):
                config = trial_config(
                    root / "campaign",
                    {"indexes": {"Scenario-8": {"path": str(root / "index.sqlite3")}}},
                    8,
                    "lookup-failure",
                    42,
                )
            operations = iter(["focus", "inspect", "nominate", "submit"])

            async def handle(**payload):
                request = decode_decision_input(json.loads(payload["input"]))
                self.assertTrue(
                    all(
                        v["lookup_status"] == "failed" and "record_status" not in v
                        for v in request["action_history"].values()
                    )
                )
                operation = next(operations)
                c = next(
                    c
                    for c in request["candidates"]
                    if c["args"].get("operation") == operation
                    and (operation == "submit" or c["args"].get("entity") == ENTITY)
                    and (
                        operation != "inspect"
                        or c["args"].get("kind") == "configuration"
                    )
                    and (
                        operation != "nominate"
                        or c["args"].get("reason") == "configuration"
                    )
                )
                return response(payload, lambda q: "action:" + c["id"])

            with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-credential"}):
                async with Application(config, root) as app:
                    incident = await app.services["normalizer"].normalize(
                        RawIncident(
                            source_id="itbench-aa",
                            event_id="lookup-failure",
                            payload={"scenario": 8, "trial": "lookup-failure"},
                        )
                    )
                    app.services["decision_provider"].provider.client_factory = (
                        lambda **kw: FakeClient(handle)
                    )
                    store = app.services["event_store"]
                    with (
                        patch.object(
                            store,
                            "action_history",
                            side_effect=ValueError("lookup failed"),
                        ),
                        patch.object(
                            store,
                            "action_count",
                            side_effect=ValueError("count unavailable"),
                        ),
                    ):
                        self.assertEqual(
                            (await app.services["incident_loop"].run(incident)).outcome,
                            "resolved",
                        )
                    history = await store.history(incident.incident_id)
                    self.assertTrue(
                        any(
                            e.kind == "validation_outcome"
                            and e.payload["count_lookup_status"]
                            == "failed_runtime_count_used"
                            for e in history
                        )
                    )

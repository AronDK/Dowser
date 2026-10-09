"""Offline typed assessments, batching, accounting and opinion-only coaching."""

import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from test_itbench_aa import ENTITY, fixture

from dowser.assessments import AssessmentQuestion, AssessmentRequest
from dowser.bench import trial_config
from dowser.bench_data import create_index
from dowser.config import Application
from dowser.contracts import AppContext
from dowser.models import Limits, RawIncident
from plugins.itbench_aa import BudgetedProvider, ProviderSettings
from plugins.jev import JevError, JevProvider, JevSettings


class AssessmentTests(unittest.IsolatedAsyncioTestCase):
    def response(self, payload):
        answers = {}
        for name, q in payload["questions"].items():
            if q["type"] == "noul":
                answers[name] = {"type": "noul", "noul": 0.5}
            elif q["type"] == "choice":
                first = next(iter(q["criteria"]))
                answers[name] = {
                    "type": "choice",
                    "choice": first,
                    "confidence": 1.0,
                    "probabilities": {k: float(k == first) for k in q["criteria"]},
                }
            else:
                legend = {str(i): v for i, v in enumerate(q["criteria"])}
                answers[name] = {
                    "type": "score",
                    "score": 0.5,
                    "confidence": 0.5,
                    "legend": legend,
                    "probabilities": {k: 0.5 for k in legend},
                }
        return {
            "model": "jev-1.13.0",
            "answers": answers,
            "usage": {"input_tokens": 10, "output_tokens": 5},
        }

    def transport(self, handle):
        original = httpx.AsyncClient
        return patch.object(
            httpx,
            "AsyncClient",
            side_effect=lambda **kw: original(
                **kw, transport=httpx.MockTransport(handle)
            ),
        )

    async def test_typed_answers_and_batch_accounting_with_retries(self):
        with tempfile.TemporaryDirectory() as root:
            provider = BudgetedProvider(
                ProviderSettings(
                    ledger="ledger.sqlite3",
                    trial="assessment",
                    retry_initial_seconds=0.001,
                ),
                AppContext({}, Limits(), Path(root)),
            )
            questions = {
                "choice": AssessmentQuestion(
                    type="choice",
                    instructions="Which evidence matters?",
                    criteria={"app": "application", "net": "network"},
                ),
                "score": AssessmentQuestion(
                    type="score",
                    instructions="How strong is support?",
                    criteria=["weak", "strong"],
                ),
                **{
                    f"noul{i}": AssessmentQuestion(
                        type="noul", instructions="Is evidence sufficient?"
                    )
                    for i in range(5)
                },
            }
            payloads = []

            def handle(request):
                payload = json.loads(request.content)
                payloads.append(payload)
                if len(payloads) == 1:
                    return httpx.Response(429, headers={"retry-after": "0.001"})
                return httpx.Response(200, json=self.response(payload))

            with (
                self.transport(handle),
                patch.dict(os.environ, {"TYPESAFE_API_KEY": "private-canary"}),
                patch("sys.stderr", io.StringIO()),
            ):
                result = await provider.assess(
                    AssessmentRequest(
                        incident_id="assessment",
                        state={"observed": True},
                        questions=questions,
                    )
                )
            self.assertEqual(set(result.answers), set(questions))
            self.assertEqual(result.answers["noul0"].noul, 0.5)
            self.assertEqual(result.answers["score"].score, 0.5)
            self.assertTrue(all(len(p["questions"]) <= 6 for p in payloads))
            stats = provider.ledger.stats()
            self.assertEqual(stats["calls"], 3)
            self.assertEqual(stats["unknown_calls"], 1)
            self.assertEqual(stats["input_tokens"], 20)
            self.assertEqual(len(provider.ledger.timings("assessment")), 3)
            self.assertNotIn("private-canary", json.dumps(payloads))
            with self.assertRaisesRegex(ValueError, "scope"):
                await provider.assess(
                    AssessmentRequest(
                        incident_id="other", state={}, questions=questions
                    )
                )

    async def test_oversized_batches_split_and_single_question_state_fail_closed(self):
        provider = JevProvider(
            JevSettings(context_window=2048, context_headroom=0),
            AppContext({}, Limits(), Path.cwd()),
        )
        payloads = []

        def handle(request):
            p = json.loads(request.content)
            payloads.append(p)
            return httpx.Response(200, json=self.response(p))

        questions = {
            f"q{i}": AssessmentQuestion(
                type="noul", instructions="Question " + "x" * 650
            )
            for i in range(6)
        }
        with (
            self.transport(handle),
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "private-canary"}),
        ):
            result = await provider.assess(
                AssessmentRequest(incident_id="split", state={}, questions=questions)
            )
            self.assertEqual(len(result.answers), 6)
            self.assertGreater(len(payloads), 1)
            count = len(payloads)
            with self.assertRaises(JevError):
                await provider.assess(
                    AssessmentRequest(
                        incident_id="split",
                        state={"data": "x" * 8192},
                        questions=questions,
                    )
                )
            with self.assertRaises(JevError):
                await provider.assess(
                    AssessmentRequest(
                        incident_id="split",
                        state={},
                        questions={
                            "one": AssessmentQuestion(
                                type="noul", instructions="x" * 3000
                            )
                        },
                    )
                )
            self.assertEqual(len(payloads), count)

    async def test_invalid_noul_score_and_choice_are_rejected(self):
        provider = JevProvider(JevSettings(), AppContext({}, Limits(), Path.cwd()))
        for q, answer in [
            (
                AssessmentQuestion(type="noul", instructions="Sufficient?"),
                {"type": "noul", "noul": "0.7"},
            ),
            (
                AssessmentQuestion(type="noul", instructions="Sufficient?"),
                {"type": "noul", "noul": float("nan")},
            ),
            (
                AssessmentQuestion(
                    type="choice", instructions="Which?", criteria={"a": "A"}
                ),
                {
                    "type": "choice",
                    "choice": "alien",
                    "confidence": 1.0,
                    "probabilities": {"a": 1.0},
                },
            ),
            (
                AssessmentQuestion(
                    type="score", instructions="Support?", criteria=["low", "high"]
                ),
                {
                    "type": "score",
                    "score": 2.0,
                    "confidence": 1.0,
                    "legend": {"0": "low", "1": "high"},
                    "probabilities": {"0": 0.5, "1": 0.5},
                },
            ),
        ]:
            request = AssessmentRequest(incident_id="bad", state={}, questions={"q": q})
            value = {
                "model": "jev-1.13.0",
                "answers": {"q": answer},
                "usage": {"input_tokens": 1, "output_tokens": 0},
            }
            with self.assertRaises(JevError):
                provider.normalize_assessment(value, request)

    async def test_coach_cache_survives_restart_and_never_grants_nomination(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            fixture(root / "source")
            create_index(root / "source", root / "index.sqlite3", 8)
            prepared = {
                "indexes": {"Scenario-8": {"path": str(root / "index.sqlite3")}}
            }
            config = trial_config(root / "campaign", prepared, 8, "coach", 42)
            config.decision_provider.settings["assessments"] = True
            async with Application(config, root) as app:
                state = await app.services["normalizer"].normalize(
                    RawIncident(
                        source_id="itbench-aa",
                        event_id="coach",
                        payload={"scenario": 8, "trial": "coach"},
                    )
                )
                store = app.services["event_store"]
                await store.ingest(state)
                registry = app.services["tool_registry"]
                candidates = await registry.candidates(state)
                focused = next(
                    c
                    for c in candidates
                    if c.args.get("operation") == "focus" and c.args["entity"] == ENTITY
                )
                transport = await registry.execute(focused, state)
                parsed = await registry.parse(focused, transport)
                await store.append(
                    "coach", "parse_outcome", {"parse": parsed.model_dump(mode="json")}
                )
                state.observations.extend(parsed.observations)
                candidates = await registry.candidates(state)
                request = await app.services["context_builder"].build(state, candidates)
                payloads = []

                def handle(http_request):
                    p = json.loads(http_request.content)
                    payloads.append(p)
                    value = self.response(p)
                    for name, answer in value["answers"].items():
                        if name in {"support", "refutation"}:
                            answer["noul"] = 0.8
                    return httpx.Response(200, json=value)

                provider = app.services["decision_provider"]
                with (
                    self.transport(handle),
                    patch.dict(os.environ, {"TYPESAFE_API_KEY": "private-canary"}),
                ):
                    first = await provider.coach.enrich(request, provider)
                    provider.coach.cache.clear()
                    second = await provider.coach.enrich(request, provider)
                self.assertEqual(len(payloads), 1)
                self.assertEqual(first.candidates, second.candidates)
                self.assertEqual(
                    set(c.id for c in first.candidates),
                    set(c.id for c in request.candidates),
                )
                self.assertFalse(
                    any(c.args.get("operation") == "nominate" for c in first.candidates)
                )
                self.assertTrue(first.memory.hypotheses[0]["contradictory"])
                memory = await store.memory(state, candidates)
                self.assertTrue(memory["hypotheses"][0]["contradictory"])
                self.assertEqual(provider.ledger.stats()["calls"], 1)
                self.assertLessEqual(
                    len(json.dumps(payloads[0]["state"]).encode()), 8192
                )

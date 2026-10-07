"""Offline Jev boundary and full-harness tests; live calls are explicit only."""

import asyncio
import importlib
import io
import json
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dowser.config import Application, Configuration, FactoryReference, load_factory
from dowser.contracts import AppContext
from dowser.models import ActionCandidate, DecisionRequest, IncidentState, Limits
from plugins.jev import JevError, JevProvider, JevSettings


class JevTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.context = AppContext({}, Limits(), self.base)
        self.provider = JevProvider(JevSettings(), self.context)
        self.state = IncidentState(
            incident_id="test:jev",
            alert={"type": "health_check"},
            desired_state={"healthy": True},
            resources=[{"id": "node", "platform": "test", "platform_version": "1"}],
        )
        self.candidate = ActionCandidate(
            id="inspect",
            tool="test.inspect",
            plugin_version="1",
            args={},
            description="Inspect approved service health",
            effect="read_only",
            resources=["node"],
            verification="health",
        )
        self.request = DecisionRequest(
            incident_id=self.state.incident_id,
            state=self.state,
            candidates=[self.candidate],
        )
        self.key = "offline-canary-credential"
        self.env = patch.dict(os.environ, {"TYPESAFE_API_KEY": self.key})
        self.env.start()
        self.network = patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        )
        self.network.start()
        self.calls = []

    async def asyncTearDown(self):
        self.network.stop()
        self.env.stop()
        self.temp.cleanup()

    def response(self, choice="c0"):
        return {
            "model": "jev-1.13.0",
            "answers": {
                "next_action": {
                    "type": "choice",
                    "choice": choice,
                    "probabilities": {
                        "c0": float(choice == "c0"),
                        "wait": float(choice == "wait"),
                        "escalate": float(choice == "escalate"),
                    },
                    "confidence": 1.0,
                }
            },
            "usage": {"input_tokens": 100, "output_tokens": 10},
        }

    def transport(self, *, body=None, status=200, error=None):
        original = httpx.AsyncClient

        def handle(request):
            self.calls.append(request)
            if error:
                raise error
            return httpx.Response(
                status, json=body if body is not None else self.response()
            )

        return patch.object(
            httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: original(
                **kwargs, transport=httpx.MockTransport(handle)
            ),
        )

    async def test_constructor_free_validation_needs_no_optional_dependency_or_key(
        self,
    ):
        with patch.object(
            importlib, "import_module", wraps=importlib.import_module
        ) as imports:
            fn, settings = load_factory(
                FactoryReference(factory="plugins.jev:decision_provider"),
                "decision_provider",
            )
        self.assertEqual(settings.model, "jev-1.13.0")
        self.assertFalse(
            any(c.args[0] in {"httpx", "dotenv"} for c in imports.call_args_list)
        )
        with patch.object(
            JevProvider, "credential", side_effect=AssertionError("credential accessed")
        ):
            provider = fn(settings, self.context)
            self.assertTrue((await provider.check_context(self.request)).fits)
            self.assertEqual((await provider.capabilities()).max_decisions_per_round, 1)

    async def test_context_check_is_local_conservative_and_preserves_snapshot(self):
        before = self.request.model_dump_json()
        check = await self.provider.check_context(self.request)
        payload, _ = self.provider.prepare(self.request)
        expected = len(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        )
        self.assertTrue(check.fits)
        self.assertEqual(check.metadata["request_bytes"], expected)
        self.assertEqual(check.metadata["accounting"], "utf8_bytes")
        self.assertEqual(self.calls, [])
        self.assertEqual(before, self.request.model_dump_json())
        large = self.request.model_copy(deep=True)
        large.state.alert["details"] = "x" * 32000
        self.assertFalse((await self.provider.check_context(large)).fits)
        with self.transport(), self.assertRaises(JevError):
            await self.provider.decide(large)
        self.assertEqual(self.calls, [])

    async def test_https_settings_and_option_limits(self):
        for endpoint in (
            "http://host",
            "https://user:secret@host",
            "https://host/path",
            "https://host?secret=1",
        ):
            with self.assertRaises(ValueError):
                JevSettings(endpoint=endpoint)
        with self.assertRaises(ValueError):
            JevSettings(max_candidates=254)
        provider = JevProvider(JevSettings(max_candidates=1), self.context)
        request = self.request.model_copy(deep=True)
        request.candidates.append(self.candidate.model_copy(update={"id": "second"}))
        self.assertFalse((await provider.check_context(request)).fits)
        with self.transport(), self.assertRaises(JevError):
            await provider.decide(request)
        self.assertEqual(self.calls, [])

    async def test_native_api_shape_maps_neutral_labels_to_snapshot_ids(self):
        with self.transport():
            result = await self.provider.decide(self.request)
        self.assertEqual(result.candidate_id, "inspect")
        self.assertEqual(result.score_metadata["usage"]["input_tokens"], 100)
        request = self.calls[0]
        self.assertEqual(request.url.path, "/v1/systemone")
        self.assertEqual(request.headers["Authorization"], "Bearer " + self.key)
        payload = json.loads(request.content)
        self.assertEqual(payload["questions"]["next_action"]["type"], "choice")
        self.assertEqual(
            set(payload["questions"]["next_action"]["criteria"]),
            {"c0", "wait", "escalate"},
        )
        self.assertNotIn(self.key, request.content.decode())
        self.assertNotIn(self.key, result.model_dump_json())
        self.assertEqual(len(self.calls), 1)

    async def test_wait_and_escalation_are_normalized(self):
        for choice in ("wait", "escalate"):
            with self.transport(body=self.response(choice)):
                result = await self.provider.decide(self.request)
            self.assertEqual(result.operation, choice)
            self.assertIsNone(result.candidate_id)
            self.assertEqual(result.wait_seconds, 1 if choice == "wait" else 0)

    async def test_dotenv_fallback_and_environment_precedence(self):
        (self.base / ".env").write_text(
            "TYPESAFE_API_KEY=file-canary-${DO_NOT_EXPAND}\n"
        )
        with patch.dict(os.environ, {"TYPESAFE_API_KEY": ""}):
            self.assertEqual(self.provider.credential(), "file-canary-${DO_NOT_EXPAND}")
        self.assertEqual(self.provider.credential(), self.key)
        with patch.dict(os.environ, {"TYPESAFE_API_KEY": ""}):
            (self.base / ".env").unlink()
            with self.assertRaises(JevError):
                self.provider.credential()

    async def test_invalid_choices_probabilities_usage_and_model_fail_closed(self):
        examples = []
        value = self.response()
        value["answers"]["next_action"]["choice"] = "unknown"
        examples.append(value)
        value = self.response()
        value["answers"]["next_action"]["probabilities"].pop("wait")
        examples.append(value)
        value = self.response()
        value["answers"]["next_action"]["probabilities"]["c0"] = -1
        examples.append(value)
        value = self.response()
        value["answers"]["next_action"]["probabilities"]["c0"] = 0.1
        examples.append(value)
        value = self.response()
        value["answers"]["next_action"]["confidence"] = 2
        examples.append(value)
        value = self.response()
        value["usage"]["input_tokens"] = "100"
        examples.append(value)
        value = self.response()
        value["model"] = "jev-different"
        examples.append(value)
        value = self.response()
        value["answers"]["next_action"]["probabilities"] = {
            "c0": 0.1,
            "wait": 0.8,
            "escalate": 0.1,
        }
        examples.append(value)
        for body in [None, [], {}, *examples]:
            with self.assertRaises(JevError):
                self.provider.normalize(
                    body, {"c0": "inspect"}, {"c0": {}, "wait": {}, "escalate": {}}
                )

    async def test_http_and_transport_errors_are_safe_with_retries_disabled(self):
        self.provider.settings.max_retries = 0
        for status in (401, 422, 429, 529):
            with (
                self.transport(status=status, body={"error": self.key}),
                self.assertRaises(JevError) as error,
            ):
                await self.provider.decide(self.request)
            self.assertNotIn(self.key, str(error.exception))
        self.assertEqual(len(self.calls), 4)
        with (
            self.transport(error=httpx.ConnectError(self.key)),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertNotIn(self.key, str(error.exception))
        self.assertEqual(len(self.calls), 5)

    async def test_rate_limit_and_overload_backoff_then_success(self):
        original = httpx.AsyncClient
        statuses = iter([429, 529, 200])
        provider_id = "11111111-2222-4333-8444-555555555555"

        def handle(request):
            self.calls.append(request)
            status = next(statuses)
            return httpx.Response(
                status,
                json=self.response() if status == 200 else {"error": self.key},
                headers={"retry-after": "0.1", "x-request-id": provider_id},
            )

        async def fast_sleep(seconds):
            delays.append(seconds)

        delays = []
        stderr = io.StringIO()
        with (
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("plugins.jev.random.uniform", return_value=1),
            patch("sys.stderr", stderr),
        ):
            result = await self.provider.decide(self.request)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(delays, [0.5, 1.0])
        self.assertEqual(result.score_metadata["attempt_count"], 3)
        self.assertEqual(
            [d["http_status"] for d in result.score_metadata["retry_failures"]],
            [429, 529],
        )
        self.assertNotIn(self.key, stderr.getvalue())
        self.assertTrue(
            all(
                json.loads(line)["will_retry"]
                for line in stderr.getvalue().splitlines()
            )
        )
        self.assertEqual(
            result.score_metadata["retry_failures"][0]["provider_request_id"],
            provider_id,
        )

    async def test_retry_exhaustion_and_retry_after_deadline(self):
        self.provider.settings.retry_initial_seconds = 0.001
        with (
            self.transport(status=429),
            patch("sys.stderr", io.StringIO()),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertEqual(len(self.calls), 3)
        self.assertTrue(error.exception.detail.retry_exhausted)
        self.assertEqual(error.exception.detail.attempt, 3)
        original = httpx.AsyncClient

        def handle(request):
            self.calls.append(request)
            return httpx.Response(
                529, json={"error": self.key}, headers={"retry-after": "60"}
            )

        with (
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("sys.stderr", io.StringIO()),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(error.exception.detail.retry_deadline_exceeded)
        self.assertFalse(error.exception.detail.retry_exhausted)

    async def test_validation_failure_diagnostics_exclude_response_values(self):
        response = self.response()
        response["answers"]["next_action"]["probabilities"]["c0"] = 0.9
        stderr = io.StringIO()
        with (
            self.transport(body=response),
            patch("sys.stderr", stderr),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(error.exception.detail.code, "probability_sum")
        self.assertEqual(error.exception.detail.http_status, 200)
        self.assertAlmostEqual(error.exception.detail.probability_sum, 0.9)
        response["answers"]["next_action"]["confidence"] = self.key
        with (
            self.transport(body=response),
            patch("sys.stderr", stderr),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertEqual(error.exception.detail.code, "answer_schema")
        self.assertEqual(error.exception.detail.schema_error_types, ["float_type"])
        self.assertEqual(error.exception.detail.schema_error_fields, ["confidence"])
        self.assertNotIn(self.key, stderr.getvalue())

    async def test_uuid_credential_is_excluded_from_request_id_diagnostics(self):
        key = "11111111-2222-4333-8444-555555555555"
        request = self.request.model_copy(update={"id": key})
        original = httpx.AsyncClient

        def handle(request):
            return httpx.Response(
                401, json={"error": key}, headers={"x-request-id": key}
            )

        stderr = io.StringIO()
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": key}),
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("sys.stderr", stderr),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(request)
        self.assertIsNone(error.exception.detail.request_id)
        self.assertIsNone(error.exception.detail.provider_request_id)
        self.assertNotIn(key, stderr.getvalue())

    async def test_bounded_renormalization_preserves_raw_values_and_choice(self):
        self.provider.settings.probability_sum_tolerance = 0.02
        response = self.response()
        response["answers"]["next_action"]["probabilities"] = {
            "c0": 0.59,
            "wait": 0.2,
            "escalate": 0.2,
        }
        stderr = io.StringIO()
        with self.transport(body=response), patch("sys.stderr", stderr):
            result = await self.provider.decide(self.request)
        self.assertEqual(result.candidate_id, "inspect")
        self.assertAlmostEqual(sum(result.score_metadata["probabilities"].values()), 1)
        self.assertEqual(
            result.score_metadata["raw_probabilities"],
            response["answers"]["next_action"]["probabilities"],
        )
        self.assertEqual(
            result.score_metadata["response_adjustments"][0]["code"],
            "probabilities_renormalized",
        )
        self.assertEqual(
            json.loads(stderr.getvalue())["kind"], "provider_response_adjustment"
        )
        for total in (0.0, 0.97, 1.03):
            response["answers"]["next_action"]["probabilities"] = {
                "c0": min(total, 1),
                "wait": max(0, total - 1),
                "escalate": 0.0,
            }
            with (
                self.transport(body=response),
                patch("sys.stderr", stderr),
                self.assertRaises(JevError),
            ):
                await self.provider.decide(self.request)

    async def test_native_choice_profile_records_advisory_probability_inconsistencies(
        self,
    ):
        self.provider.settings.strict_probabilities = False
        self.provider.settings.probability_sum_tolerance = 0.02
        response = self.response()
        response["answers"]["next_action"]["probabilities"] = {
            "c0": 0.35,
            "wait": 0.36,
            "escalate": 0.28,
        }
        with self.transport(body=response), patch("sys.stderr", io.StringIO()):
            result = await self.provider.decide(self.request)
        self.assertEqual(result.candidate_id, "inspect")
        codes = {d["code"] for d in result.score_metadata["response_adjustments"]}
        self.assertEqual(
            codes, {"choice_probability_mismatch", "probabilities_renormalized"}
        )
        response["answers"]["next_action"]["probabilities"] = {
            "c0": 0.1,
            "wait": 0.2,
            "escalate": 0.0,
        }
        with self.transport(body=response), patch("sys.stderr", io.StringIO()):
            result = await self.provider.decide(self.request)
        self.assertEqual(result.candidate_id, "inspect")
        self.assertEqual(
            result.score_metadata["probabilities"],
            result.score_metadata["raw_probabilities"],
        )
        self.assertIn(
            "probability_sum_warning",
            {d["code"] for d in result.score_metadata["response_adjustments"]},
        )
        for invalid in ("unknown", "options", "range", "model"):
            body = self.response()
            if invalid == "unknown":
                body["answers"]["next_action"]["choice"] = self.key
            if invalid == "options":
                body["answers"]["next_action"]["probabilities"].pop("wait")
            if invalid == "range":
                body["answers"]["next_action"]["probabilities"]["c0"] = -1.0
            if invalid == "model":
                body["model"] = "jev-wrong"
            with (
                self.transport(body=body),
                patch("sys.stderr", io.StringIO()),
                self.assertRaises(JevError),
            ):
                await self.provider.decide(self.request)

    async def test_sdk_retry_statuses_transport_and_millisecond_retry_after(self):
        original = httpx.AsyncClient
        statuses = iter([408, 503, 200])

        def handle(request):
            self.calls.append(request)
            status = next(statuses)
            return httpx.Response(
                status, json=self.response() if status == 200 else {"error": self.key}
            )

        async def fast_sleep(seconds):
            pass

        with (
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("sys.stderr", io.StringIO()),
        ):
            result = await self.provider.decide(self.request)
        self.assertEqual(result.score_metadata["attempt_count"], 3)
        self.assertEqual(
            [d["http_status"] for d in result.score_metadata["retry_failures"]],
            [408, 503],
        )
        attempt = 0

        def transport_failure(request):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise httpx.ConnectError(self.key)
            return httpx.Response(200, json=self.response())

        with (
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(transport_failure)
                ),
            ),
            patch("plugins.jev.asyncio.sleep", side_effect=fast_sleep),
            patch("sys.stderr", io.StringIO()),
        ):
            result = await self.provider.decide(self.request)
        self.assertEqual(result.score_metadata["attempt_count"], 2)
        self.assertEqual(
            result.score_metadata["retry_failures"][0]["cause_type"], "ConnectError"
        )
        with patch("plugins.jev.random.uniform", return_value=1):
            self.assertEqual(self.provider.retry_delay(1, "60", "2500"), 2.5)

    async def test_request_deadline_has_structured_transport_detail(self):
        self.provider.settings.timeout_seconds = 0.03
        original = httpx.AsyncClient

        async def handle(request):
            self.calls.append(request)
            await asyncio.sleep(1)

        with (
            patch.object(
                httpx,
                "AsyncClient",
                side_effect=lambda **kwargs: original(
                    **kwargs, transport=httpx.MockTransport(handle)
                ),
            ),
            patch("sys.stderr", io.StringIO()),
            self.assertRaises(JevError) as error,
        ):
            await self.provider.decide(self.request)
        self.assertEqual(error.exception.detail.code, "provider_deadline")
        self.assertEqual(len(self.calls), 1)

    async def test_missing_optional_dependency_and_credential_in_facts(self):
        from plugins import jev

        with (
            patch.object(jev.importlib, "import_module", side_effect=ImportError),
            self.assertRaisesRegex(JevError, "dowser\\[jev\\]"),
        ):
            await self.provider.decide(self.request)
        request = self.request.model_copy(deep=True)
        request.state.alert["detail"] = self.key
        with self.transport(), self.assertRaises(JevError):
            await self.provider.decide(request)
        self.assertEqual(self.calls, [])

    async def test_provider_outage_escalates_in_real_harness_with_no_execution(self):
        from pydantic import ConfigDict

        from dowser.contracts import Component, ToolSpec
        from dowser.core import DefaultRegistry
        from dowser.models import Boundary

        class Args(Boundary):
            model_config = ConfigDict(strict=True, extra="forbid")

        class Plugin(Component):
            tools = (
                ToolSpec(
                    "test.inspect",
                    "1",
                    Args,
                    "read_only",
                    "observation",
                    {"test": ("1",)},
                    ("health",),
                ),
            )

            async def candidates(inner, state):
                return [self.candidate]

            async def validate(inner, c, state):
                from dowser.models import ValidationResult

                return ValidationResult(allowed=True)

            async def execute(inner, c, state):
                raise AssertionError("unsolicited execution")

            async def parse(inner, c, result):
                raise AssertionError("not called")

            async def verify(inner, state, c, result):
                raise AssertionError("not called")

            async def recover(inner, state, c, result):
                return []

        config = Configuration.model_validate(
            {
                "event_store": {
                    "factory": "dowser.store:sqlite_store",
                    "settings": {"path": "history.sqlite3"},
                },
                "tool_registry": {"factory": "dowser.core:tool_registry"},
                "context_builder": {"factory": "dowser.core:context_builder"},
                "decision_provider": {"factory": "plugins.jev:decision_provider"},
                "validation_policy": {"factory": "dowser.core:validation_policy"},
                "executor": {"factory": "dowser.core:executor"},
                "verifier": {"factory": "dowser.core:verifier"},
                "incident_loop": {"factory": "dowser.loop:incident_loop"},
            }
        )
        with self.transport(status=503):
            async with Application(config, self.base) as app:
                app.services["tool_registry"].plugins = [Plugin()]
                app.services["tool_registry"].tools = DefaultRegistry([Plugin()]).tools
                terminal = await app.services["incident_loop"].run(self.state)
                history = await app.services["event_store"].history(
                    terminal.incident_id
                )
        self.assertEqual(terminal.outcome, "escalated")
        self.assertFalse(any(e.kind == "execution_start" for e in history))
        self.assertTrue(any(e.kind == "terminated" for e in history))
        failure = next(
            e.payload["failure"] for e in history if e.kind == "provider_failure"
        )
        self.assertEqual(failure["http_status"], 503)
        self.assertEqual(failure["code"], "http_error")
        self.assertNotIn(
            self.key, json.dumps([e.model_dump(mode="json") for e in history])
        )
        self.assertNotIn(
            self.key, json.dumps([e.model_dump(mode="json") for e in history])
        )


if __name__ == "__main__":
    unittest.main()

"""Escalation controls preserve native evidence and never bypass action scope."""

import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from pydantic import ValidationError

from dowser.bench import DEFAULTS, main, trial_config
from dowser.config import FactoryReference, load_factory
from dowser.contracts import AppContext, Component, factory
from dowser.core import (
    ContextSettings,
    DefaultContextBuilder,
    PolicySettings,
    validation_policy,
)
from dowser.loop import DefaultIncidentLoop
from dowser.models import (
    ActionCandidate,
    Boundary,
    DecisionRequest,
    DecisionResult,
    IncidentState,
    Limits,
    ValidationResult,
)
from dowser.store import SQLiteStore
from plugins.escalation import Policy, Settings
from plugins.jev import JevError, JevProvider, JevSettings, decision_provider


class Empty(Boundary):
    pass


class OutsidePolicy(Component):
    controls = Settings()

    async def apply(self, request, decision, alternatives):
        return DecisionResult(operation="select", candidate_id="outside-scope")


@factory(
    subsystem="decision_policy", component_type=OutsidePolicy, settings_model=Empty
)
def outside_policy(settings, context):
    return OutsidePolicy()


def request():
    state = IncidentState(
        incident_id="incident",
        alert={"error": "dependency unavailable"},
        desired_state={"diagnosed": True},
        resources=[{"id": "service"}],
    )
    candidate = ActionCandidate(
        id="inspect",
        tool="fixture.inspect",
        plugin_version="1",
        args={},
        description="Inspect the dependency",
        effect="read_only",
        resources=["service"],
        verification="health",
    )
    return DecisionRequest(
        incident_id=state.incident_id, state=state, candidates=[candidate]
    )


def response(choice, probabilities):
    return {
        "model": "jev-1.13.0",
        "answers": {
            "next_action": {
                "type": "choice",
                "choice": choice,
                "probabilities": probabilities,
                "confidence": 0.4,
            }
        },
        "usage": {"input_tokens": 100, "output_tokens": 10},
    }


class PenaltyTests(unittest.IsolatedAsyncioTestCase):
    async def apply(self, penalty, probabilities, operation="escalate"):
        native = DecisionResult(
            operation=operation,
            candidate_id="inspect" if operation == "select" else None,
            score_metadata={"raw_probabilities": probabilities, "confidence": 0.4},
        )
        alternatives = {
            "c0": DecisionResult(operation="select", candidate_id="inspect"),
            "wait": DecisionResult(operation="wait", wait_seconds=2),
        }
        return await Policy(Settings(penalty=penalty)).apply(
            request(), native, alternatives
        ), native

    async def test_penalty_one_preserves_native_choice_even_when_scores_disagree(self):
        result, native = await self.apply(1, {"escalate": 0.3, "c0": 0.6, "wait": 0.1})
        self.assertEqual(result.operation, native.operation)
        self.assertFalse(result.score_metadata["escalation_policy"]["adjusted"])

    async def test_higher_penalty_can_choose_supplied_action_and_preserve_scores(self):
        result, native = await self.apply(3, {"escalate": 0.6, "c0": 0.3, "wait": 0.1})
        self.assertEqual(result.candidate_id, "inspect")
        self.assertEqual(
            result.score_metadata["raw_probabilities"],
            native.score_metadata["raw_probabilities"],
        )
        self.assertTrue(result.score_metadata["escalation_policy"]["adjusted"])
        self.assertEqual(native.operation, "escalate")

    async def test_penalty_keeps_strong_escalation_and_never_changes_native_action(
        self,
    ):
        result, _ = await self.apply(2, {"escalate": 0.9, "c0": 0.05, "wait": 0.05})
        self.assertEqual(result.operation, "escalate")
        result, _ = await self.apply(
            2, {"escalate": 0.9, "c0": 0.05, "wait": 0.05}, "select"
        )
        self.assertEqual(result.candidate_id, "inspect")

    async def test_supplied_wait_is_preserved_as_an_alternative(self):
        result, _ = await self.apply(3, {"escalate": 0.6, "c0": 0.1, "wait": 0.3})
        self.assertEqual(result.operation, "wait")
        self.assertEqual(result.wait_seconds, 2)

    async def test_increasing_penalty_never_increases_escalation(self):
        decisions = [
            (await self.apply(p, {"escalate": 0.6, "c0": 0.3, "wait": 0.1}))[
                0
            ].operation
            for p in (1, 2, 4, 10)
        ]
        self.assertEqual(decisions, ["escalate", "escalate", "select", "select"])

    async def test_invalid_penalties_and_disabled_escalation_fail_closed(self):
        for value in (0, -1, 0.5, float("inf"), float("nan")):
            with self.assertRaises(ValidationError):
                Settings(penalty=value)
        with self.assertRaises(ValueError):
            await Policy(Settings(enabled=False)).apply(
                request(), DecisionResult(operation="escalate"), {}
            )


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_loop_rejects_disabled_escalation_without_executing_any_action(self):
        provider = JevProvider(JevSettings(allow_escalation=False), self.context)
        s = request().state
        c = request().candidates[0]

        class Registry:
            async def candidates(inner, state):
                return [c]

            async def validate(inner, candidate, state):
                return ValidationResult(allowed=True)

            async def execute(inner, *args):
                raise AssertionError("unexpected execution")

        registry = Registry()
        store = SQLiteStore(Path(self.tmp.name) / "history.sqlite3")
        self.addAsyncCleanup(store.aclose)
        services = {
            "event_store": store,
            "tool_registry": registry,
            "context_builder": DefaultContextBuilder(ContextSettings(), store),
            "decision_provider": provider,
            "validation_policy": validation_policy(PolicySettings(), self.context),
            "executor": registry,
            "verifier": registry,
        }

        async def forged(view):
            return DecisionResult(operation="escalate", reason="forged escalation")

        with patch.object(provider, "decide", forged):
            result = await DefaultIncidentLoop(
                AppContext(services, Limits(), Path(self.tmp.name))
            ).run(s)
        events = await store.history(s.incident_id)
        self.assertIn("provider failed", result.reason)
        self.assertTrue(any(e.kind == "provider_failure" for e in events))
        self.assertFalse(any(e.kind == "execution_started" for e in events))

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.context = AppContext({}, Limits(), Path(self.tmp.name))
        self.env = patch.dict(os.environ, {"TYPESAFE_API_KEY": "offline-credential"})
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def transport(self, body):
        original = httpx.AsyncClient
        return patch(
            "httpx.AsyncClient",
            side_effect=lambda **kwargs: original(
                **kwargs,
                transport=httpx.MockTransport(
                    lambda req: httpx.Response(200, json=body)
                ),
            ),
        )

    async def test_disabled_option_is_absent_and_forged_escalation_rejected(self):
        provider = JevProvider(JevSettings(allow_escalation=False), self.context)
        payload, _ = provider.prepare(request())
        self.assertNotIn("escalate", payload["questions"]["next_action"]["criteria"])
        self.assertFalse((await provider.capabilities()).metadata["allow_escalation"])
        with self.transport(response("c0", {"c0": 0.9, "wait": 0.1})):
            result = await provider.decide(request())
        self.assertEqual(result.candidate_id, "inspect")
        with (
            self.transport(
                response("escalate", {"c0": 0.1, "wait": 0.1, "escalate": 0.8})
            ),
            self.assertRaises(JevError) as caught,
        ):
            await provider.decide(request())
        self.assertEqual(caught.exception.detail.code, "unknown_choice")

    async def test_factory_configures_plugin_and_records_native_and_adjusted_decisions(
        self,
    ):
        settings = JevSettings(
            escalation_policy=FactoryReference(
                factory="plugins.escalation:escalation_policy", settings={"penalty": 3}
            )
        )
        provider = await decision_provider(settings, self.context)
        self.addAsyncCleanup(provider.aclose)
        with self.transport(
            response("escalate", {"c0": 0.3, "wait": 0.1, "escalate": 0.6})
        ):
            result = await provider.decide(request())
        self.assertEqual(result.candidate_id, "inspect")
        self.assertEqual(
            result.score_metadata["native_decision"]["operation"], "escalate"
        )
        self.assertEqual(result.score_metadata["native_choice"], "escalate")
        self.assertTrue(result.score_metadata["escalation_policy"]["adjusted"])

    async def test_config_validation_is_constructor_free_and_custom_policy_cannot_invent_action(
        self,
    ):
        ref = FactoryReference(
            factory="plugins.jev:decision_provider",
            settings={
                "escalation_policy": {"factory": "test_escalation:outside_policy"}
            },
        )
        reference_factory = outside_policy
        with patch(
            "test_escalation.outside_policy",
            side_effect=AssertionError("construction forbidden"),
        ) as mocked:
            mocked.interface_version = reference_factory.interface_version
            mocked.subsystem = reference_factory.subsystem
            mocked.component_type = reference_factory.component_type
            mocked.settings_model = reference_factory.settings_model
            mocked.dependencies = reference_factory.dependencies
            load_factory(ref, "decision_provider")
        provider = await decision_provider(
            JevSettings.model_validate(ref.settings), self.context
        )
        self.addAsyncCleanup(provider.aclose)
        with (
            self.transport(response("c0", {"c0": 0.8, "wait": 0.1, "escalate": 0.1})),
            self.assertRaises(JevError) as caught,
        ):
            await provider.decide(request())
        self.assertEqual(caught.exception.detail.code, "decision_policy_failed")

    async def test_benchmark_profile_disables_escalation_and_records_penalty(self):
        config = trial_config(
            Path(self.tmp.name) / "campaign",
            {"indexes": {"Scenario-8": {"path": "fixture.sqlite3"}}},
            8,
            "pilot-s8",
            42,
        )
        settings = config.decision_provider.settings
        self.assertFalse(settings["allow_escalation"])
        self.assertFalse(config.normalizer.settings["allow_escalation"])
        self.assertEqual(
            settings["escalation_policy"]["settings"]["penalty"],
            DEFAULTS["escalation_penalty"],
        )
        self.assertFalse(DEFAULTS["allow_model_escalation"])


class CliTests(unittest.TestCase):
    def test_cli_records_explicit_penalty_in_profile_without_paid_calls(self):
        original = copy.deepcopy(DEFAULTS)
        seen = []

        async def run(*args, **kwargs):
            seen.append(copy.deepcopy(DEFAULTS))
            return Path("fixture")

        try:
            with (
                patch("dowser.bench.run_campaign", side_effect=run),
                patch("dowser.bench.report", return_value={"complete": False}),
                patch("builtins.print"),
            ):
                self.assertEqual(
                    main(
                        [
                            "run",
                            "--phase",
                            "pilot",
                            "--allow-model-escalation",
                            "--escalation-penalty",
                            "3",
                        ]
                    ),
                    0,
                )
            self.assertTrue(seen[0]["allow_model_escalation"])
            self.assertEqual(seen[0]["escalation_penalty"], 3)
        finally:
            DEFAULTS.clear()
            DEFAULTS.update(original)

    def test_penalty_without_explicit_enable_is_rejected_before_runner(self):
        with (
            patch("dowser.bench.run_campaign", new_callable=AsyncMock) as run,
            patch("sys.stderr"),
            self.assertRaises(SystemExit),
        ):
            main(["run", "--phase", "pilot", "--escalation-penalty", "3"])
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()

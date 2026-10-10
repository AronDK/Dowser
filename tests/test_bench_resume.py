"""Audited batch-only revisions and transient continuation preserve trials."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from dowser.bench import (
    DEFAULTS,
    failure_category,
    model_refusal,
    normalize_report_row,
    record_runner_update,
    report,
    run_campaign,
    run_trial,
    runner_execution_fingerprint,
    transient_full_failure,
)
from dowser.bench_data import PILOT, atomic_json, file_hash, fingerprint
from dowser.bench_ledger import Ledger


class ResumeTests(unittest.TestCase):
    def test_native_refusal_is_distinct_from_schema_and_transport_failures(self):
        self.assertTrue(
            model_refusal(
                {
                    "category": "provider",
                    "failure": {"category": "response", "code": "refusal"},
                }
            )
        )
        for code in ("answer_schema", "unknown_choice", "malformed_response"):
            self.assertFalse(
                model_refusal(
                    {
                        "category": "provider",
                        "failure": {"category": "response", "code": code},
                    }
                )
            )
        self.assertFalse(model_refusal({"category": "interrupted", "failure": None}))

    def test_successfully_trimmed_context_does_not_override_model_escalation(self):
        class Accounting:
            call_limit = None

            def last_error(self, trial):
                return None

        terminal = {"outcome": "escalated", "reason": "Jev selected escalation"}
        events = [
            {"kind": "context_check", "payload": {"fits": False}},
            {"kind": "context_check", "payload": {"fits": True}},
        ]
        self.assertEqual(
            failure_category(events, terminal, Accounting(), "trial"), "abandoned"
        )
        original = {"category": "context", "terminal": terminal}
        self.assertEqual(normalize_report_row(original)["category"], "abandoned")
        self.assertEqual(original["category"], "context")
        terminal = {
            "outcome": "escalated",
            "reason": "required incident context exceeds provider limits: too large",
        }
        self.assertEqual(
            failure_category(events[:1], terminal, Accounting(), "trial"), "context"
        )

    def test_transient_failures_continue_but_permanent_and_schema_failures_halt(self):
        for status in (408, 429, 503, 520, 529):
            self.assertTrue(
                transient_full_failure(
                    {
                        "category": "provider",
                        "failure": {
                            "category": "http",
                            "retryable": True,
                            "http_status": status,
                        },
                    }
                )
            )
        for status in (401, 403, 422):
            self.assertFalse(
                transient_full_failure(
                    {
                        "category": "provider",
                        "failure": {
                            "category": "http",
                            "retryable": False,
                            "http_status": status,
                        },
                    }
                )
            )
        self.assertFalse(
            transient_full_failure(
                {
                    "category": "provider",
                    "failure": {"category": "response", "code": "answer_schema"},
                }
            )
        )
        self.assertTrue(
            transient_full_failure(
                {
                    "category": "provider",
                    "failure": {"category": "timeout", "code": "deadline_exceeded"},
                }
            )
        )
        self.assertFalse(
            transient_full_failure(
                {"category": "interrupted", "failure": {"code": "request_cancelled"}}
            )
        )

    def test_runner_fingerprint_rejects_per_trial_and_configuration_changes(self):
        original = "DEFAULTS={'model': 'jev-1.13.0'}\ndef run_trial():\n return 1\ndef run_campaign():\n return 1\n"
        wrapper = original.replace(
            "def run_campaign():\n return 1", "def run_campaign():\n return 2"
        )
        self.assertEqual(
            runner_execution_fingerprint(original),
            runner_execution_fingerprint(wrapper),
        )
        execution = original.replace(
            "def run_trial():\n return 1", "def run_trial():\n return 2"
        )
        self.assertNotEqual(
            runner_execution_fingerprint(original),
            runner_execution_fingerprint(execution),
        )
        self.assertNotEqual(
            runner_execution_fingerprint(original),
            runner_execution_fingerprint(original.replace("1.13.0", "latest")),
        )

    def test_authorized_revision_is_archived_without_replacing_initial_fingerprint(
        self,
    ):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            campaign = base / "campaign"
            original = campaign / "source/dowser/bench.py"
            original.parent.mkdir(parents=True)
            original.write_text(
                "def run_trial():\n return 1\ndef run_campaign():\n return 1\n"
            )
            current = base / "dowser/bench.py"
            current.parent.mkdir()
            current.write_text(
                original.read_text().replace(
                    "def run_campaign():\n return 1", "def run_campaign():\n return 2"
                )
            )
            before = {
                "dowser/bench.py": file_hash(original),
                "plugins/jev.py": "unchanged",
            }
            after = {**before, "dowser/bench.py": file_hash(current)}
            existing = {"code_hashes": before.copy(), "methodological_differences": []}
            expected = {"code_hashes": after}
            atomic_json(campaign / "manifest.json", existing)
            with self.assertRaises(ValueError):
                record_runner_update(campaign, existing, expected, base, False)
            record_runner_update(campaign, existing, expected, base, True)
            self.assertEqual(existing["code_hashes"], before)
            self.assertEqual(
                file_hash(Path(existing["runner_revisions"][0]["source"])),
                after["dowser/bench.py"],
            )
            record_runner_update(campaign, existing, expected, base, False)
            self.assertEqual(len(existing["runner_revisions"]), 1)
            with self.assertRaises(ValueError):
                record_runner_update(
                    campaign,
                    existing,
                    {"code_hashes": {**after, "plugins/jev.py": "modified"}},
                    base,
                    True,
                )


class PilotContinuationTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_refusal_is_not_replayed_and_unrun_cases_advance(self):
        await self.check_saved_cases(False, refusal=True)

    async def test_saved_deadline_failure_is_not_replayed_and_six_unrun_cases_advance(
        self,
    ):
        await self.check_saved_cases(False)

    async def test_user_interrupted_case_is_retained_and_only_five_unrun_cases_advance(
        self,
    ):
        await self.check_saved_cases(True)

    async def check_saved_cases(self, interrupted, refusal=False):
        saved_count = 5 if interrupted else 4
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "uv.lock").write_text("fixture lock")
            (base / "pyproject.toml").write_text("fixture source")
            root = base / "runs"
            root.mkdir()
            prepared = {
                "dataset": str(base),
                "files": {},
                "indexes": {},
                "scenarios": [],
                "license": "fixture",
            }
            prepared["fingerprint"] = fingerprint(prepared)
            atomic_json(root / "prepared.json", prepared)
            campaign = root / "continuation"
            campaign.mkdir()
            ledger = Ledger(campaign / "spending.sqlite3")
            saved_hashes = {}
            for i, scenario in enumerate(PILOT[:saved_count]):
                trial = f"pilot-s{scenario}"
                call = ledger.reserve(trial)
                if i != 3:
                    ledger.reconcile(
                        call, {"input_tokens": 100, "output_tokens": 10}, 0
                    )
                category = (
                    "provider" if i == 3 else "completed" if i == 2 else "abandoned"
                )
                if i == 4:
                    category = "interrupted"
                row = {
                    "trial": trial,
                    "scenario": scenario,
                    "seed": 42,
                    "phase": "pilot",
                    "status": "completed",
                    "category": category,
                    "score": float(i == 2),
                    "exact_root_set": i == 2,
                    "dataset_fingerprint": prepared["fingerprint"],
                    "configuration_fingerprint": fingerprint(DEFAULTS),
                    **ledger.stats(trial),
                }
                if i == 3:
                    row["failure"] = {
                        "code": "refusal" if refusal else "deadline_exceeded",
                        "category": "response" if refusal else "timeout",
                        "retryable": False,
                    }
                path = campaign / "trials" / (trial + ".json")
                atomic_json(path, row)
                saved_hashes[path] = file_hash(path)
            new_cases = []

            async def trial_driver(
                campaign, prepared, scenario, trial, seed, phase, base
            ):
                if scenario in PILOT[:saved_count]:
                    # Exercise the real early-return path for completed paid cases.
                    return await run_trial(
                        campaign, prepared, scenario, trial, seed, phase, base
                    )
                new_cases.append(scenario)
                call = ledger.reserve(trial)
                ledger.reconcile(call, {"input_tokens": 100, "output_tokens": 10}, 0)
                row = {
                    "trial": trial,
                    "scenario": scenario,
                    "phase": phase,
                    "status": "completed",
                    "category": "completed",
                    "score": 1.0,
                    "exact_root_set": True,
                    **ledger.stats(trial),
                }
                atomic_json(campaign / "trials" / (trial + ".json"), row)
                return row

            cooldown = AsyncMock()
            with (
                patch("dowser.bench.run_trial", side_effect=trial_driver),
                patch("dowser.bench.freeze_price_source", return_value="fixture-price"),
                patch("dowser.bench.asyncio.sleep", cooldown),
                patch("builtins.print"),
            ):
                if interrupted:
                    await run_campaign(root, "continuation", phase="pilot", base=base)
                    self.assertEqual(
                        new_cases,
                        [],
                        "interrupted cases must require explicit continuation",
                    )
                await run_campaign(
                    root,
                    "continuation",
                    phase="pilot",
                    base=base,
                    continue_interrupted=interrupted,
                )
            self.assertEqual(new_cases, list(PILOT[saved_count:]))
            self.assertEqual(ledger.stats()["calls"], 10)
            self.assertEqual(ledger.stats()["unknown_calls"], 1)
            self.assertEqual({p: file_hash(p) for p in saved_hashes}, saved_hashes)
            self.assertEqual(
                cooldown.await_count, 2 if interrupted else 0 if refusal else 1
            )
            summary = report(campaign)
            self.assertTrue(summary["pilot_finished"])
            self.assertEqual(summary["full_trials"], 0)
            self.assertEqual(summary["pilot_categories"]["provider"], 1)
            self.assertAlmostEqual(
                summary["pilot_mean_score"], 0.6 if interrupted else 0.7
            )
            if interrupted:
                self.assertEqual(summary["pilot_categories"]["interrupted"], 1)
            self.assertEqual(
                json.loads(
                    (campaign / "trials" / f"pilot-s{PILOT[3]}.json").read_text()
                )["category"],
                "provider",
            )


if __name__ == "__main__":
    unittest.main()

"""Opt-in real Jev memory validation, followed by the isolated ten-case pilot.

Run with ``uv run --extra jev --extra itbench-aa python -m dowser.validate_memory
--campaign NAME --previous-campaign PATH --pilot``. Existing results never replay.
"""

import argparse
import asyncio
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from plugins.jev import JevProvider, JevSettings

from .bench import (
    archive_sources,
    code_metadata,
    freeze_price_source,
    report,
    run_campaign,
)
from .bench_data import atomic_json, dumps
from .bench_ledger import Ledger
from .contracts import AppContext
from .core import ContextSettings, DefaultContextBuilder
from .diagnostics import failure_details
from .models import (
    ActionCandidate,
    IncidentState,
    Limits,
    MemoryFact,
    MemoryScope,
    Observation,
    ParseResult,
    Resource,
)
from .rate_limit import shared_rate_limiter
from .store import SQLiteStore

FIXED_TIME = datetime(2026, 1, 1, tzinfo=UTC)
FAMILIES = ("navigation", "previous_failure", "related_alert")


class TrialAccounting:
    """Per-case accounting independent of the model-visible incident identity."""

    def __init__(self, ledger, trial):
        self.ledger, self.trial = ledger, trial

    def start(self, request_id, attempt):
        return self.ledger.reserve(self.trial, request_id=request_id, attempt=attempt)

    def success(self, call_id, usage, latency):
        self.ledger.reconcile(call_id, usage, latency)

    def failure(self, call_id, error, latency):
        self.ledger.failure(call_id, error, latency)


def current_state(family, incident):
    return IncidentState(
        incident_id=incident,
        alert={"service": "payment", "symptom": "requests return HTTP 503"},
        resources=[Resource(id="payment", platform="fixture", platform_version="1")],
        desired_state={
            "next_step": "investigate the upstream cause supported by earlier evidence"
        },
        instructions=[
            "Select the most relevant diagnostic probe using cumulative evidence and prior outcomes. Do not repeat a failed probe. Historical findings require a fresh confirming probe. Both network and application configuration remain plausible from the current alert alone."
        ],
        observations=[
            Observation(
                id="current",
                observed_at=FIXED_TIME,
                resource_id="payment",
                kind="current_alert",
                payload={
                    "error": "HTTP 503; no upstream cause identified in this latest observation"
                },
            )
        ],
        memory_scope=MemoryScope(
            namespace="live-memory-validation", partition=incident
        ),
    )


def choices():
    return [
        ActionCandidate(
            id=name,
            tool="fixture.diagnose",
            plugin_version="1",
            args={"probe": name},
            description=description,
            effect="read_only",
            resources=["payment"],
            verification="fixture",
            created_at=FIXED_TIME,
        )
        for name, description in (
            (
                "network",
                "Inspect network routes and firewall rules for the payment service's upstream connection.",
            ),
            (
                "configuration",
                "Inspect the payment application's configuration and mounted ConfigMap values.",
            ),
            (
                "repeat",
                "Repeat the same earlier dependency probe without changing its parameters.",
            ),
        )
    ]


async def focused_request(path, family, condition, incident):
    store = SQLiteStore(path)
    try:
        s = current_state(family, incident)
        previous = s.model_copy(deep=True)
        previous.incident_id = (
            incident + "-previous" if family == "related_alert" else incident
        )
        previous.observations = []
        await store.ingest(previous)
        if family == "previous_failure":
            c = choices()[2]
            await store.append(
                previous.incident_id,
                "execution_started",
                {"execution_id": "probe", "candidate": c.model_dump(mode="json")},
            )
            cause = (
                "network route validation failed: required gateway route is missing"
                if condition == "network"
                else "application configuration validation failed: upstream URL is malformed"
            )
            await store.append(
                previous.incident_id,
                "execution_result",
                {
                    "execution_id": "probe",
                    "tool": c.tool,
                    "status": "failed",
                    "detail": cause,
                    "parse": {
                        "status": "skipped",
                        "parser_version": "fixture/1",
                        "reason": "probe failed; investigate the indicated cause instead of repeating it",
                    },
                },
            )
        else:
            finding = (
                {
                    "route_to_upstream": "missing",
                    "mounted_application_configuration": "validated correct",
                }
                if condition == "network"
                else {
                    "route_to_upstream": "validated correct",
                    "mounted_application_configuration": "upstream URL contains a wrong hostname",
                }
            )
            await store.append(
                previous.incident_id,
                "parse_outcome",
                {
                    "execution_id": "fixture",
                    "parse": ParseResult(
                        status="valid",
                        parser_version="fixture/1",
                        memory_facts=[
                            MemoryFact(
                                key="earlier-diagnostic-finding",
                                resource_id="payment",
                                payload=finding,
                            )
                        ],
                    ).model_dump(mode="json"),
                },
            )
        if family == "related_alert":
            await store.ingest(s)
    finally:
        await store.aclose()
    # Reopen deliberately: memory must come from durable SQLite, not local state.
    store = SQLiteStore(path)
    try:
        request = await DefaultContextBuilder(
            ContextSettings(recent_outcomes=0), store
        ).build(s, choices())
        # Navigation and a later observation have removed the earlier observation
        # from the active state. Only the assembled memory supplies its content.
        return request
    finally:
        await store.aclose()


async def run_focused(directory, base, previous_campaign=None):
    directory = Path(directory).absolute()
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "manifest.json").exists():
        raise ValueError(
            "focused validation already started; paid cases never automatically replay"
        )
    pricing_hash = freeze_price_source(directory)
    hashes = code_metadata(base)
    archive_sources(directory, base, hashes)
    ledger = Ledger(directory / "spending.sqlite3", budget=None, call_limit=None)
    if previous_campaign:
        ledger.inherit(
            Ledger.prior_snapshot(
                Path(previous_campaign).absolute() / "spending.sqlite3"
            )
        )
    atomic_json(
        directory / "manifest.json",
        {
            "model": "jev-1.13.0",
            "code_hashes": hashes,
            "pricing_source_sha256": pricing_hash,
            "families": FAMILIES,
            "repetitions": 5,
            "required_correct_per_condition": 4,
            "memory_omitted_controls": 15,
            "budget_usd": None,
            "rates": {"requests_per_second": 80, "tokens_per_second": 100000},
        },
    )
    rows = []
    for family in FAMILIES:
        for repetition in range(5):
            # Pair current inputs and candidates exactly, changing only memory.
            incident = f"{family}-{repetition}"
            for condition in ("network", "configuration"):
                request = await focused_request(
                    directory / "fixtures" / f"{incident}-{condition}.sqlite3",
                    family,
                    condition,
                    incident,
                )
                cases = [("memory", request)]
                if condition == ("network" if repetition % 2 == 0 else "configuration"):
                    control = request.model_copy(deep=True)
                    control.memory = None
                    cases.append(("control", control))
                for mode, view in cases:
                    trial = f"{incident}-{condition}-{mode}"
                    settings = JevSettings(
                        strict_probabilities=False, probability_sum_tolerance=0.02
                    )
                    provider = JevProvider(
                        settings,
                        AppContext({}, Limits(), base),
                        accounting=TrialAccounting(ledger, trial),
                        rate_limiter=shared_rate_limiter(str(ledger.path), 80, 100000),
                    )
                    payload, _ = provider.prepare(view)
                    atomic_json(
                        directory / "requests" / f"{trial}.json",
                        {
                            "request": view.model_dump(mode="json"),
                            "native_payload": payload,
                        },
                    )
                    row = {
                        "trial": trial,
                        "family": family,
                        "condition": condition,
                        "mode": mode,
                        "expected": condition,
                        "passed": False,
                    }
                    try:
                        result = await provider.decide(view)
                        row.update(
                            result=result.model_dump(mode="json"),
                            passed=result.candidate_id == condition,
                            category="behavior",
                        )
                    except Exception as exc:
                        row.update(
                            category="provider_failure", failure=failure_details(exc)
                        )
                    finally:
                        await provider.aclose()
                    row["accounting"] = ledger.stats(trial)
                    rows.append(row)
                    atomic_json(directory / "results" / f"{trial}.json", row)
                    print(
                        dumps(
                            {
                                "trial": trial,
                                "passed": row["passed"],
                                "category": row["category"],
                            }
                        ),
                        flush=True,
                    )
    counts = {
        f"{family}/{condition}": sum(
            r["passed"]
            for r in rows
            if r["family"] == family
            and r["condition"] == condition
            and r["mode"] == "memory"
        )
        for family in FAMILIES
        for condition in ("network", "configuration")
    }
    if code_metadata(base) != hashes:
        raise ValueError("source changed during focused validation")
    controls = [r for r in rows if r["mode"] == "control"]
    summary = {
        "passed": all(n >= 4 for n in counts.values())
        and not any(r["category"] == "provider_failure" for r in rows),
        "memory_correct_per_condition": counts,
        "control_correct": sum(r["passed"] for r in controls),
        "control_cases": len(controls),
        "cases": len(rows),
        "categories": dict(Counter(r["category"] for r in rows)),
        "ledger": ledger.stats(),
    }
    atomic_json(directory / "summary.json", summary)
    (directory / "summary.md").write_text(
        "# Actual Jev memory validation\n\n"
        + f"Passed: **{summary['passed']}**. Memory decisions per condition (required 4/5): `{dumps(counts)}`.\n\nMemory-omitted controls: {summary['control_correct']}/{summary['control_cases']}; their failure is not required. Cases: {len(rows)}.\n\n"
        + f"Cumulative accounted cost: ${summary['ledger']['accounted_nanodollars'] / 1e9:.9f}; no spending ceiling. Calls in validation: {summary['ledger']['calls']}.\n\nRequests, native payloads, actual choices, failures and usage are retained next to this report.\n"
    )
    return summary


async def run(args):
    base = Path.cwd().absolute()
    directory = (args.root / args.campaign).absolute()
    if (directory / "manifest.json").exists() or (
        directory / "focused" / "manifest.json"
    ).exists():
        raise ValueError(
            "validation directory already contains a run; paid cases never automatically replay"
        )
    status_path = directory / "validation-status.json"
    atomic_json(
        status_path,
        {"status": "focused_running", "started_at": datetime.now(UTC).isoformat()},
    )
    try:
        focused = await run_focused(directory / "focused", base, args.previous_campaign)
        atomic_json(
            status_path,
            {
                "status": "focused_passed" if focused["passed"] else "focused_failed",
                "focused": focused,
            },
        )
        if not focused["passed"]:
            return 2
        if args.pilot:
            atomic_json(status_path, {"status": "pilot_running", "focused": focused})
            campaign = await run_campaign(
                args.root,
                args.campaign,
                phase="pilot",
                base=base,
                previous_campaign=directory / "focused",
            )
            summary = report(campaign)
            atomic_json(
                status_path,
                {
                    "status": "completed"
                    if summary["pilot_finished"]
                    else "stopped_incomplete",
                    "focused": focused,
                    "pilot": summary,
                    "finished_at": datetime.now(UTC).isoformat(),
                },
            )
            return 0 if summary["pilot_finished"] else 2
        return 0
    except Exception as exc:
        atomic_json(status_path, {"status": "failed", "failure": failure_details(exc)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(".local/itbench-aa"))
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--previous-campaign", type=Path)
    parser.add_argument("--pilot", action="store_true")
    args = parser.parse_args()
    if not args.campaign or any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for c in args.campaign
    ):
        parser.error("invalid campaign name")
    try:
        return asyncio.run(run(args))
    except Exception as exc:
        print(
            dumps({"kind": "validation_failure", "failure": failure_details(exc)}),
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

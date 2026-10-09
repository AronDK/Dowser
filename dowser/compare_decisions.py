"""Offline identical-fixture comparisons and read-only pilot navigation replay."""

import argparse
import asyncio
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from plugins.itbench_aa import Plugin, Settings, working
from plugins.jev import JevProvider, JevSettings
from plugins.openai_decisions import OpenAIDecisionsProvider, OpenAISettings

from .bench_data import atomic_json, file_hash
from .contracts import AppContext
from .core import ContextSettings, DefaultContextBuilder
from .memory import digest
from .models import ActionCandidate, IncidentState, Limits, MemoryFact, ParseResult
from .store import SQLiteStore


async def compare(output, *, pilot=None, prepared=None):
    output.mkdir(parents=True, exist_ok=True)
    store = SQLiteStore(output / "fixtures.sqlite3", output / "fixtures.jsonl")
    state = IncidentState(
        incident_id="comparison",
        alert={"service": "payment", "symptom": "HTTP 503"},
        resources=[{"id": "payment"}],
        desired_state={},
        instructions=[
            "Facts and previous outcomes inform model choice. Repetition does not disqualify authorized actions."
        ],
    )
    candidates = [
        ActionCandidate(
            id=f"probe-{i}",
            tool="fixture.probe",
            plugin_version="1",
            args={"entity": f"dependency-{i}"},
            description=f"Inspect observed dependency {i}",
            effect="read_only",
            resources=["payment"],
            verification="check",
        )
        for i in range(8)
    ]
    try:
        await store.ingest(state)
        for i in range(300):
            await store.append(
                state.incident_id,
                "parse_outcome",
                {
                    "parse": ParseResult(
                        status="valid",
                        parser_version="fixture/1",
                        memory_facts=[
                            MemoryFact(
                                key=f"finding-{i}",
                                resource_id="payment",
                                payload={
                                    "entity": f"dependency-{i % 8}",
                                    "kind": "logs",
                                    "finding": i,
                                    "content": f"Observed dependency sample {i}. Connection failures remain uncertain; no diagnosis has been confirmed.",
                                },
                            )
                        ],
                    ).model_dump(mode="json")
                },
            )
        context = AppContext({"event_store": store}, Limits(), output)
        providers = {
            "openai": OpenAIDecisionsProvider(OpenAISettings(), context),
            "jev": JevProvider(
                JevSettings(allow_escalation=False, token_accounting="estimate"),
                context,
            ),
        }
        rows = []
        for profile in ("bounded", "large"):
            request = await DefaultContextBuilder(
                ContextSettings(profile=profile), store
            ).build(state, candidates)
            atomic_json(
                output / f"{profile}-request.json", request.model_dump(mode="json")
            )
            for name, provider in providers.items():
                check = await provider.check_context(request)
                if name == "openai":
                    rendered = {
                        "model": "gpt-6-luna",
                        "input": provider.input(request),
                        "questions": provider.batches(candidates)[0],
                    }
                else:
                    rendered, _ = provider.prepare(request)
                atomic_json(output / f"{profile}-{name}-input.json", rendered)
                rows.append(
                    {
                        "provider": name,
                        "profile": profile,
                        "state_sha256": digest(request.state.model_dump(mode="json")),
                        "candidate_sha256": digest(
                            [c.model_dump(mode="json") for c in request.candidates]
                        ),
                        "visible_facts": len(request.memory.facts),
                        "omitted_facts": request.memory.progress["omitted_facts"],
                        "context_check": check.model_dump(mode="json"),
                        "model_calls": 0,
                    }
                )
        replay = []
        if pilot:
            if not prepared:
                raise ValueError("pilot replay requires the original prepared manifest")
            before = file_hash(pilot)
            indexes = json.loads(prepared.read_text())["indexes"]
            seen_incidents = set()
            with closing(
                sqlite3.connect(
                    pilot.absolute().as_uri() + "?mode=ro&immutable=1", uri=True
                )
            ) as db:
                for incident, sequence, payload in db.execute(
                    "SELECT incident_id,sequence,payload FROM events WHERE kind='provider_request' ORDER BY incident_id,sequence"
                ):
                    if incident in seen_incidents:
                        continue
                    request = json.loads(payload)["request"]
                    operations = {
                        c["args"].get("operation") for c in request["candidates"]
                    }
                    if operations != {"nominate"}:
                        continue
                    seen_incidents.add(incident)
                    state = IncidentState.model_validate(request["state"])
                    scenario = state.payload["scenario"]
                    plugin = Plugin(
                        Settings(
                            index=indexes[f"Scenario-{scenario}"]["path"],
                            scenario=scenario,
                            trial=incident,
                            output=str(output / "unused-submission.json"),
                        ),
                        AppContext({}, Limits(), output),
                    )
                    plugin.revision = working(state)["revision"]
                    catalogue = await plugin.candidates(state)
                    new = {c.args.get("operation") for c in catalogue}
                    assert "browse" in new and "focus" in new and "inspect" in new
                    replay.append(
                        {
                            "incident_id": incident,
                            "source_sequence": sequence,
                            "previous_operations": sorted(operations),
                            "new_operations": sorted(new),
                            "candidates": len(catalogue),
                            "browser_available": True,
                            "focus_entities": len(
                                {
                                    c.args["entity"]
                                    for c in catalogue
                                    if c.args.get("operation") == "focus"
                                }
                            ),
                            "model_calls": 0,
                        }
                    )
                    # One reproduction per stalled incident is sufficient.
                    if sum(r["incident_id"] == incident for r in replay) > 1:
                        replay.pop()
            assert file_hash(pilot) == before
        result = {
            "fixtures": rows,
            "navigation_replay": replay,
            "model_calls": 0,
            "accuracy_measured": False,
            "sources": [
                "https://developers.openai.com/api/docs/guides/decisions",
                "https://developers.openai.com/api/docs/models/gpt-6-luna",
                "https://developers.openai.com/api/reference/resources/decisions/methods/create",
                "https://docs.typesafe.ai/models",
            ],
            "interpretation": "Input coverage and navigation are harness properties. This comparison does not measure model diagnostic quality.",
        }
        atomic_json(output / "comparison.json", result)
        return result
    finally:
        await store.aclose()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pilot-history", type=Path)
    parser.add_argument("--prepared", type=Path)
    args = parser.parse_args(argv)
    result = asyncio.run(
        compare(args.output, pilot=args.pilot_history, prepared=args.prepared)
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

"""Registered read-only history queries and owned artifact retrieval."""

from dowser.contracts import Component, ToolSpec, factory
from dowser.history import HistoryQuery
from dowser.memory import digest
from dowser.models import (
    ActionCandidate,
    Boundary,
    Observation,
    ParseResult,
    TransportResult,
    ValidationResult,
    VerificationResult,
)


class Settings(Boundary):
    pass


class HistoryPlugin(Component):
    tools = (
        ToolSpec(
            "history.query",
            "1",
            HistoryQuery,
            "read_only",
            "observation",
            {"*": ("*",)},
            ("history",),
        ),
    )

    def __init__(self, context):
        self.store = context.require("event_store")

    async def candidates(self, state):
        queries = [
            {},
            {"category": "fact"},
            {"category": "action"},
            {"category": "rejection"},
            {"category": "choice"},
        ]
        queries.extend(
            {"outcome": status}
            for status in ("succeeded", "failed", "partial", "unknown", "rejected")
        )
        memory = await self.store.memory(state, [], 12)
        for fact in memory["facts"]:
            entity = fact["payload"].get("entity", fact["resource_id"])
            queries.append({"entity": entity})
        # Concrete pagination and artifact options come only from admitted results.
        for obs in state.observations:
            if obs.kind != "history":
                continue
            result = obs.payload["result"]
            if result.get("next_offset") is not None:
                queries.append(
                    {**obs.payload["query"], "offset": result["next_offset"]}
                )
            for entry in result.get("entries", []):
                for ref in entry.get("raw_output_refs", []):
                    queries.append({"artifact_ref": ref})
                for key in (
                    "entity",
                    "tool",
                    "outcome",
                    "evidence_kind",
                    "text",
                    "after",
                    "before",
                ):
                    value = entry.get(
                        "outcome_status"
                        if key == "outcome"
                        else "entity"
                        if key == "text"
                        else "timestamp"
                        if key in {"after", "before"}
                        else key
                    )
                    if value and isinstance(value, str):
                        queries.append({key: value})
        unique = {
            digest(q): HistoryQuery.model_validate(q).model_dump(exclude_defaults=True)
            for q in queries
        }
        return [
            ActionCandidate(
                id="history-" + key[:24],
                tool="history.query",
                plugin_version="1",
                args=query,
                description="Retrieve admitted incident history: " + str(query),
                effect="read_only",
                resources=[r.id for r in state.resources],
                verification="history",
            )
            for key, query in unique.items()
        ]

    async def validate(self, candidate, state):
        try:
            HistoryQuery.model_validate(candidate.args)
            self.store.history_scope(state)
            if candidate.args.get("artifact_ref") and not self.store.saved_result(
                state.incident_id, [candidate.args["artifact_ref"]]
            ):
                raise ValueError("unowned artifact")
            return ValidationResult(allowed=True)
        except ValueError:
            return ValidationResult(
                allowed=False, reason="invalid or out-of-scope history query"
            )

    async def execute(self, candidate, state):
        try:
            result = await self.store.query_history(state, candidate.args)
        except Exception as exc:
            return TransportResult(
                status="failed",
                raw_output={
                    "query": candidate.args,
                    "result": {
                        "lookup_status": "failed",
                        "error_type": type(exc).__name__,
                    },
                },
                detail="History lookup failed; no absence of previous records is inferred",
            )
        return TransportResult(
            status="succeeded", raw_output={"query": candidate.args, "result": result}
        )

    async def parse(self, candidate, result):
        return ParseResult(
            status="valid",
            parser_version="history/1",
            observations=[
                Observation(
                    resource_id=candidate.resources[0],
                    kind="history",
                    payload=result.raw_output,
                )
            ],
        )

    async def verify(self, state, candidate, result):
        return VerificationResult(
            status="inconclusive",
            reason="retrieved saved history; original observation freshness is unchanged",
        )

    async def recover(self, state, candidate, result):
        return []


@factory(
    subsystem="tool_plugin",
    component_type=HistoryPlugin,
    settings_model=Settings,
    dependencies=("event_store",),
)
def tool_plugin(settings, context):
    return HistoryPlugin(context)

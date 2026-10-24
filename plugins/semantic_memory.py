"""Dynamic read-only semantic recall and exact saved-evidence expansion."""

from pydantic import ConfigDict, Field

from dowser.contracts import ToolSpec, factory
from dowser.models import (
    ActionCandidate,
    Boundary,
    ParseResult,
    TransportResult,
    ValidationResult,
)

from .history import HistoryPlugin, Settings


class SearchArgs(Boundary):
    model_config = ConfigDict(strict=True, extra="forbid")
    intent: str = Field(pattern=r"^[0-9a-f]{64}$")


class SemanticMemoryPlugin(HistoryPlugin):
    tools = HistoryPlugin.tools + (
        ToolSpec(
            "memory.search",
            "1",
            SearchArgs,
            "read_only",
            "observation",
            {"*": ("*",)},
            ("history",),
        ),
    )

    async def candidates(self, state):
        # Exact artifact/filter/pagination access remains available without vector recall.
        return await super().candidates(state)

    async def recall_candidates(self, state, candidates):
        intents = await self.store.memory_query_intents(state, candidates)
        return [
            ActionCandidate(
                id="memory-" + key[:24],
                tool="memory.search",
                plugin_version="1",
                args={"intent": key},
                description="Retrieve saved evidence for "
                + value["source"]
                + ": "
                + value["text"],
                effect="read_only",
                resources=[r.id for r in state.resources],
                verification="history",
            )
            for key, value in intents.items()
        ]

    async def validate(self, candidate, state):
        if candidate.tool == "history.query":
            return await super().validate(candidate, state)
        try:
            args = SearchArgs.model_validate(candidate.args)
            with self.store.lock:
                self.store.history_scope(state)
                if args.intent not in self.store.intents.get(state.incident_id, {}):
                    raise ValueError("expired intent")
            return ValidationResult(allowed=True)
        except ValueError:
            return ValidationResult(
                allowed=False, reason="invalid or expired memory intent"
            )

    async def execute(self, candidate, state):
        if candidate.tool == "history.query":
            return await super().execute(candidate, state)
        result = await self.store.search_memory(state, candidate.args["intent"])
        return TransportResult(
            status="succeeded",
            raw_output=result,
            detail="Saved evidence retrieved; original freshness is unchanged",
        )

    async def parse(self, candidate, result):
        if candidate.tool == "history.query":
            return await super().parse(candidate, result)
        # Recall is an output artifact, not a new live observation. Automatic context
        # continues to select from the original admitted sources.
        return ParseResult(status="valid", parser_version="semantic/1")


@factory(
    subsystem="tool_plugin",
    component_type=SemanticMemoryPlugin,
    settings_model=Settings,
    dependencies=("event_store",),
)
def tool_plugin(settings, context):
    return SemanticMemoryPlugin(context)

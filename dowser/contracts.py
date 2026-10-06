"""Public extension contracts and declarative factory metadata."""

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

from . import INTERFACE_VERSION
from .models import (
    ActionCandidate,
    ContextCheck,
    DecisionBatch,
    DecisionCapabilities,
    DecisionRequest,
    DecisionResult,
    Event,
    ExecutionResult,
    IncidentState,
    Limits,
    ParseResult,
    PendingIncident,
    RawIncident,
    TerminalResult,
    TransportResult,
    ValidationResult,
    VerificationResult,
)


class Component:
    interface_version = INTERFACE_VERSION

    async def aclose(self) -> None:
        """Release resources; safe even after incomplete initialization."""


@dataclass(frozen=True)
class AppContext:
    services: Mapping[str, Any]
    limits: Limits
    base_dir: Path

    def require(self, name: str) -> Any:
        return self.services[name]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    plugin_version: str
    argument_model: type[BaseModel]
    effect: str
    kind: str
    platforms: Mapping[str, tuple[str, ...]]
    verification_hooks: tuple[str, ...]
    recovery_hooks: tuple[str, ...] = ()


class ToolPlugin(Protocol):
    interface_version: str
    tools: tuple[ToolSpec, ...]

    async def candidates(self, state: IncidentState) -> list[ActionCandidate]: ...
    async def validate(
        self, candidate: ActionCandidate, state: IncidentState
    ) -> ValidationResult: ...
    async def execute(
        self, candidate: ActionCandidate, state: IncidentState
    ) -> TransportResult: ...
    async def parse(
        self, candidate: ActionCandidate, result: TransportResult
    ) -> ParseResult: ...
    async def verify(
        self, state: IncidentState, candidate: ActionCandidate, result: ExecutionResult
    ) -> VerificationResult: ...
    async def recover(
        self, state: IncidentState, candidate: ActionCandidate, result: ExecutionResult
    ) -> list[ActionCandidate]: ...
    async def aclose(self) -> None: ...


class DecisionProvider(Protocol):
    interface_version: str

    async def check_context(self, request: DecisionRequest) -> ContextCheck: ...
    async def decide(
        self, request: DecisionRequest
    ) -> DecisionResult | DecisionBatch: ...
    async def aclose(self) -> None: ...


class DecisionCapabilityProvider(Protocol):
    """Optional additional contract for providers returning multiple decisions."""

    async def capabilities(self) -> DecisionCapabilities: ...


class EventStore(Protocol):
    interface_version: str

    async def ingest(self, state: IncidentState) -> Event: ...
    async def append(
        self, incident_id: str, kind: str, payload: dict, artifacts: dict | None = None
    ) -> Event: ...
    async def history(self, incident_id: str) -> list[Event]: ...
    async def reconstruct(self, incident_id: str) -> IncidentState: ...
    async def inspect(self, incident_id: str) -> dict: ...
    async def aclose(self) -> None: ...


class ToolRegistry(Protocol):
    interface_version: str

    async def candidates(self, state: IncidentState) -> list[ActionCandidate]: ...
    async def validate(
        self, candidate: ActionCandidate, state: IncidentState
    ) -> ValidationResult: ...
    async def execute(
        self, candidate: ActionCandidate, state: IncidentState
    ) -> TransportResult: ...
    async def parse(
        self, candidate: ActionCandidate, result: TransportResult
    ) -> ParseResult: ...
    async def verify(
        self, state: IncidentState, candidate: ActionCandidate, result: ExecutionResult
    ) -> VerificationResult: ...
    async def recover(
        self, state: IncidentState, candidate: ActionCandidate, result: ExecutionResult
    ) -> list[ActionCandidate]: ...
    async def aclose(self) -> None: ...


class ContextBuilder(Protocol):
    interface_version: str

    async def build(
        self, state: IncidentState, candidates: list[ActionCandidate]
    ) -> DecisionRequest: ...
    async def trim(self, request: DecisionRequest) -> DecisionRequest | None: ...
    async def aclose(self) -> None: ...


class ValidationPolicy(Protocol):
    interface_version: str

    async def validate(
        self, candidate: ActionCandidate, state: IncidentState, budget: dict
    ) -> ValidationResult: ...
    async def aclose(self) -> None: ...


class Executor(Protocol):
    interface_version: str

    async def execute(
        self, candidate: ActionCandidate, state: IncidentState
    ) -> TransportResult: ...
    async def aclose(self) -> None: ...


class Verifier(Protocol):
    interface_version: str

    async def verify(
        self, state: IncidentState, candidate: ActionCandidate, result: ExecutionResult
    ) -> VerificationResult: ...
    async def aclose(self) -> None: ...


class IncidentLoop(Protocol):
    interface_version: str

    async def run(self, state: IncidentState) -> TerminalResult: ...
    async def aclose(self) -> None: ...


class IncidentSource(Protocol):
    interface_version: str

    async def open(self) -> AsyncIterator[RawIncident]: ...
    async def checkpoint(self, record: RawIncident, result: TerminalResult) -> None: ...
    async def aclose(self) -> None: ...


class Normalizer(Protocol):
    interface_version: str

    async def normalize(self, record: RawIncident) -> IncidentState | None: ...
    async def aclose(self) -> None: ...


class Scheduler(Protocol):
    interface_version: str

    async def select(self, pending: Sequence[PendingIncident]) -> str: ...
    async def aclose(self) -> None: ...


# Positional arities, excluding self. validate checks these without instantiation.
INTERFACES = {
    "event_store": {
        "ingest": 1,
        "append": 4,
        "history": 1,
        "reconstruct": 1,
        "inspect": 1,
    },
    "tool_registry": {
        "candidates": 1,
        "validate": 2,
        "execute": 2,
        "parse": 2,
        "verify": 3,
        "recover": 3,
    },
    "context_builder": {"build": 2, "trim": 1},
    "decision_provider": {"check_context": 1, "decide": 1},
    "validation_policy": {"validate": 3},
    "executor": {"execute": 2},
    "verifier": {"verify": 3},
    "incident_loop": {"run": 1},
    "incident_source": {"open": 0, "checkpoint": 2},
    "normalizer": {"normalize": 1},
    "scheduler": {"select": 1},
}
INTERFACES["tool_plugin"] = INTERFACES["tool_registry"]
OPTIONAL_INTERFACES = {"decision_provider": {"capabilities": 0}}


def factory(
    *,
    subsystem: str,
    component_type: type,
    settings_model: type[BaseModel],
    dependencies: tuple[str, ...] = (),
) -> Callable:
    """Declare compatibility without running a constructor or accessing tools."""

    def decorate(fn):
        fn.interface_version = INTERFACE_VERSION
        fn.subsystem = subsystem
        fn.component_type = component_type
        fn.settings_model = settings_model
        fn.dependencies = dependencies
        return fn

    return decorate

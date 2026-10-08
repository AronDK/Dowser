"""Versioned JSON boundaries. Provider-specific encodings belong to adapters."""

from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


def now() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return str(uuid4())


class Boundary(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


Phase = Literal[
    "observe",
    "select",
    "validate",
    "execute",
    "verify",
    "wait",
    "execution_failed",
    "recover",
    "resolved",
    "recovery_unverified",
    "escalated",
]


class Resource(Boundary):
    id: str = Field(min_length=1)
    platform: str = "unspecified"
    platform_version: str = "unspecified"
    payload: dict[str, JsonValue] = Field(default_factory=dict)


class Observation(Boundary):
    id: str = Field(default_factory=new_id, min_length=1)
    observed_at: datetime = Field(default_factory=now)
    resource_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    payload: dict[str, JsonValue]
    evidence_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def aware(self):
        if self.observed_at.tzinfo is None:
            raise ValueError("observation timestamps must have a timezone")
        return self


class MemoryScope(Boundary):
    namespace: str = Field(min_length=1)
    partition: str = Field(min_length=1)


class MemoryFact(Boundary):
    key: str = Field(min_length=1)
    resource_id: str = Field(min_length=1)
    payload: dict[str, JsonValue]
    evidence_refs: list[str] = Field(default_factory=list)
    status: Literal["observed", "contradicted", "superseded"] = "observed"


class ActionIdentity(Boundary):
    key: str = Field(min_length=1)
    evidence_version: str = ""
    decision_state: str = ""


class InvestigationMemory(Boundary):
    hypotheses: list[dict[str, JsonValue]] = Field(default_factory=list)
    facts: list[dict[str, JsonValue]] = Field(default_factory=list)
    actions: list[dict[str, JsonValue]] = Field(default_factory=list)
    progress: dict[str, JsonValue] = Field(default_factory=dict)
    historical: bool = False


class IncidentState(Boundary):
    schema_version: Literal["1"] = "1"
    incident_id: str = Field(min_length=1)
    alert: dict[str, JsonValue]
    desired_state: dict[str, JsonValue]
    resources: list[Resource] = Field(min_length=1)
    instructions: list[str] = Field(default_factory=list)
    observations: list[Observation] = Field(default_factory=list)
    attempts: list[dict[str, JsonValue]] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    phase: Phase = "observe"
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    memory_scope: MemoryScope | None = None

    @model_validator(mode="after")
    def unique_resources(self):
        if len({r.id for r in self.resources}) != len(self.resources):
            raise ValueError("duplicate incident resources")
        if len({o.id for o in self.observations}) != len(self.observations):
            raise ValueError("duplicate incident observation IDs")
        if any(
            o.resource_id not in {r.id for r in self.resources}
            for o in self.observations
        ):
            raise ValueError("observation outside incident scope")
        return self


class RawIncident(Boundary):
    """Transient source envelope; never automatically persisted or sent to models."""

    source_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    payload: JsonValue
    metadata: dict[str, JsonValue] | None = None


class PendingIncident(Boundary):
    """Scheduler snapshot, deliberately excluding source checkpoint handles."""

    queue_id: str = Field(min_length=1)
    arrival_sequence: int = Field(ge=1)
    state: IncidentState


def alert_value(alert: dict, field: str):
    """Prefer an optional canonical value, falling back to its native value."""
    canonical = alert.get(f"canonical_{field}")
    return alert.get(field) if canonical is None else canonical


def mapped_value(mapping: dict, value, default=None):
    """JSON object keys represent string or numeric platform values."""
    if isinstance(value, str):
        return mapping.get(value, default)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return mapping.get(str(value), default)
    return default


def normalize_incident(data: dict) -> IncidentState:
    """Accept the note's JSON; keep unknown platform fields in a payload."""
    data = dict(data)
    if "recent_attempts" in data:
        if "attempts" in data:
            raise ValueError("use attempts or recent_attempts, not both")
        data["attempts"] = data.pop("recent_attempts")
    if not data.get("resources"):
        alert = data.get("alert", {})
        target = alert.get("device_id") or alert.get("resource_id")
        if not target:
            raise ValueError(
                "explicit resources or alert.device_id/resource_id required"
            )
        data["resources"] = [
            {
                "id": str(target),
                "platform": alert.get("platform", "unspecified"),
                "platform_version": alert.get("platform_version", "unspecified"),
                "payload": {k: alert[k] for k in ("interface",) if k in alert},
            }
        ]
    extras = {k: data.pop(k) for k in list(data) if k not in IncidentState.model_fields}
    data["payload"] = {**data.get("payload", {}), **extras}
    state = IncidentState.model_validate(data)
    if state.phase != "observe" or state.attempts:
        raise ValueError("new incidents must start in observe without prior attempts")
    return state


class ActionCandidate(Boundary):
    id: str = Field(min_length=1)
    tool: str = Field(min_length=1)
    plugin_version: str
    args: dict[str, JsonValue]
    description: str = Field(min_length=1)
    effect: Literal["read_only", "change"]
    kind: Literal["observation", "remediation", "recovery"] = "observation"
    resources: list[str] = Field(min_length=1)
    preconditions: list[str] = Field(default_factory=list)
    required_observation_ids: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=now)
    timeout_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    verification: str = Field(min_length=1)
    recovery: str | None = None

    @model_validator(mode="after")
    def aware(self):
        if self.created_at.tzinfo is None:
            raise ValueError("candidate timestamps must have a timezone")
        return self


class DecisionRequest(Boundary):
    schema_version: Literal["1"] = "1"
    id: str = Field(default_factory=new_id)
    incident_id: str
    state: IncidentState
    candidates: list[ActionCandidate]
    memory: InvestigationMemory | None = None


class ContextCheck(Boundary):
    fits: bool
    reason: str = ""
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class DecisionResult(Boundary):
    operation: Literal["select", "wait", "escalate"]
    candidate_id: str | None = None
    reason: str = ""
    wait_seconds: float = Field(default=0, ge=0, allow_inf_nan=False)
    score_metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def coherent(self):
        if (self.operation == "select") != (self.candidate_id is not None):
            raise ValueError("only select requires a candidate_id")
        if self.operation == "wait" and self.wait_seconds <= 0:
            raise ValueError("wait requires a positive duration")
        if self.operation != "wait" and self.wait_seconds:
            raise ValueError("only wait accepts wait_seconds")
        return self


class DecisionCapabilities(Boundary):
    """Decision volume advertised by the selected model's provider adapter."""

    max_decisions_per_round: int = Field(ge=1)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class DecisionBatch(Boundary):
    """Ordered decisions from one provider call; wait/escalate ends the batch."""

    decisions: list[DecisionResult] = Field(min_length=1)

    @model_validator(mode="after")
    def terminal_last(self):
        if any(decision.operation != "select" for decision in self.decisions[:-1]):
            raise ValueError("wait or escalation must be the final batch decision")
        return self


class ValidationResult(Boundary):
    allowed: bool
    reason: str = ""


class ProcedureStep(Boundary):
    step_id: str = Field(min_length=1)
    target: list[str] = Field(min_length=1)
    started_at: datetime
    finished_at: datetime
    status: Literal["succeeded", "failed", "partial", "unknown"]
    parse_status: Literal["valid", "failed", "missing", "skipped"] = "skipped"
    detail: str = ""
    raw_output: JsonValue = None
    raw_output_refs: list[str] = Field(default_factory=list)


class TransportResult(Boundary):
    status: Literal["succeeded", "failed", "partial", "unknown"]
    transport_status: Literal["completed", "failed", "unknown"] = "completed"
    command_exit_code: int | None = None
    raw_output: JsonValue = None
    detail: str = ""
    steps: list[ProcedureStep] = Field(default_factory=list)

    @model_validator(mode="after")
    def coherent_outcome(self):
        if self.status == "succeeded" and (
            self.transport_status != "completed"
            or self.command_exit_code not in (None, 0)
            or any(step.status != "succeeded" for step in self.steps)
        ):
            raise ValueError(
                "successful execution contradicts transport/command/step outcomes"
            )
        if (
            any(step.status == "unknown" for step in self.steps)
            and self.status != "unknown"
        ):
            raise ValueError(
                "unknown procedure step requires an unknown overall outcome"
            )
        if len({step.step_id for step in self.steps}) != len(self.steps):
            raise ValueError("duplicate procedure step IDs")
        return self


class ParseResult(Boundary):
    status: Literal["valid", "failed", "missing", "skipped"]
    parser_version: str
    observations: list[Observation] = Field(default_factory=list)
    reason: str = ""
    memory_facts: list[MemoryFact] = Field(default_factory=list)

    @model_validator(mode="after")
    def evidence_only_on_valid(self):
        if self.status != "valid" and (self.observations or self.memory_facts):
            raise ValueError("failed parsing cannot emit observations")
        return self


class ExecutionResult(Boundary):
    execution_id: str
    action_id: str
    tool: str
    target: list[str]
    started_at: datetime
    finished_at: datetime
    status: Literal["succeeded", "failed", "partial", "unknown"]
    transport_status: Literal["completed", "failed", "unknown"] = "completed"
    command_exit_code: int | None = None
    parse: ParseResult
    raw_output_refs: list[str] = Field(default_factory=list)
    detail: str = ""
    steps: list[ProcedureStep] = Field(default_factory=list)


class VerificationResult(Boundary):
    status: Literal["passed", "failed", "inconclusive"]
    reason: str
    evidence_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def affirmative_evidence(self):
        if self.status == "passed" and not self.evidence_refs:
            raise ValueError("passed verification requires evidence references")
        return self


class TerminalResult(Boundary):
    schema_version: Literal["1"] = "1"
    incident_id: str
    outcome: Literal["resolved", "escalated", "recovery_unverified"]
    reason: str
    evidence_refs: list[str] = Field(default_factory=list)


class Event(Boundary):
    schema_version: Literal["1"] = "1"
    incident_id: str
    sequence: int
    timestamp: datetime
    kind: str
    payload: dict[str, JsonValue]


class Limits(Boundary):
    incident_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)
    tool_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    identical_attempts: int = Field(default=2, ge=1)
    changes: int = Field(default=0, ge=0)
    freshness_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def obsolete_round_limit(cls, value):
        # Preserve loading old configuration files without retaining a core cap.
        if isinstance(value, dict) and "decision_rounds" in value:
            value = dict(value)
            value.pop("decision_rounds")
        return value

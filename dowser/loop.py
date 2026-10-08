"""One-incident orchestration with durable authorization and outcome boundaries."""

import asyncio
import time
from itertools import count

from .contracts import Component, factory
from .core import EmptySettings, required_view
from .diagnostics import failure_details
from .memory import action_identity
from .models import (
    ActionCandidate,
    ActionIdentity,
    ContextCheck,
    DecisionBatch,
    DecisionCapabilities,
    DecisionRequest,
    DecisionResult,
    ExecutionResult,
    ParseResult,
    TerminalResult,
    TransportResult,
    ValidationResult,
    VerificationResult,
    new_id,
    now,
)
from .runtime import bounded_call
from .store import reject_credentials


class DefaultIncidentLoop(Component):
    def __init__(self, context):
        self.services = context.services
        self.limits = context.limits
        self.running = False

    async def run(self, state):
        if self.running:
            raise RuntimeError("default loop processes one incident at a time")
        if state.phase != "observe" or state.attempts:
            raise ValueError(
                "new incidents must start in observe without prior attempts"
            )
        self.running = True
        try:
            return await self._run(state.model_copy(deep=True))
        finally:
            self.running = False

    async def _run(self, state):
        store = self.services["event_store"]
        registry = self.services["tool_registry"]
        builder = self.services["context_builder"]
        provider = self.services["decision_provider"]
        policy = self.services["validation_policy"]
        executor = self.services["executor"]
        verifier = self.services["verifier"]
        deadline = time.monotonic() + self.limits.incident_seconds
        attempts = {}
        identities = {}
        minimum_memory = None
        changes = 0
        pending_recovery = None
        active = None
        evidence = set()

        def remaining():
            return deadline - time.monotonic()

        async def call(fn, *args, seconds=None):
            return await bounded_call(
                fn, *args, seconds=min(remaining(), seconds) if seconds else remaining()
            )

        async def record(kind, payload, artifacts=None):
            event = await store.append(state.incident_id, kind, payload, artifacts)
            evidence.add(f"{state.incident_id}:{event.sequence}")
            evidence.update((artifacts or {}).keys())
            return event

        async def phase(value):
            state.phase = value
            await record("state_transition", {"phase": value})

        async def terminate(reason, outcome=None, refs=None):
            outcome = outcome or ("recovery_unverified" if changes else "escalated")
            result = TerminalResult(
                incident_id=state.incident_id,
                outcome=outcome,
                reason=reason,
                evidence_refs=refs or sorted(evidence),
            )
            await phase(outcome)
            await record("terminated", result.model_dump(mode="json"))
            return result

        async def gate(candidate, stage):
            identity_hook = getattr(registry, "action_identity", None)
            identity = ActionIdentity.model_validate(
                await call(identity_hook, candidate, state)
                if identity_hook
                else action_identity(candidate)
            )
            identities[candidate.id] = identity
            count_hook = getattr(store, "action_count", None)
            attempted = (
                await count_hook(state.incident_id, identity)
                if count_hook
                else attempts.get(identity.key, 0)
            )
            budget = {
                "seconds_remaining": remaining(),
                "attempts": attempted,
                "changes": changes,
            }
            if budget["seconds_remaining"] <= 0:
                result = ValidationResult(
                    allowed=False, reason="incident deadline exhausted"
                )
            elif budget["attempts"] >= self.limits.identical_attempts:
                result = ValidationResult(
                    allowed=False, reason="identical action attempt limit reached"
                )
            elif candidate.effect == "change" and changes >= self.limits.changes:
                result = ValidationResult(
                    allowed=False, reason="change budget exhausted"
                )
            else:
                result = ValidationResult.model_validate(
                    await call(
                        registry.validate,
                        candidate.model_copy(deep=True),
                        state.model_copy(deep=True),
                    )
                )
                if result.allowed:
                    result = ValidationResult.model_validate(
                        await call(
                            policy.validate,
                            candidate.model_copy(deep=True),
                            state.model_copy(deep=True),
                            budget,
                        )
                    )
            await record(
                "validation_outcome",
                {
                    "action_id": candidate.id,
                    "stage": stage,
                    **result.model_dump(mode="json"),
                },
            )
            return result

        def protected(request, candidates):
            # Enforce the context boundary even for replacement context builders.
            if (
                request.incident_id != state.incident_id
                or request.state.incident_id != state.incident_id
            ):
                raise ValueError("context builder changed incident identity")
            for field in (
                "schema_version",
                "alert",
                "desired_state",
                "resources",
                "instructions",
                "unresolved_questions",
                "payload",
                "phase",
                "memory_scope",
            ):
                if getattr(request.state, field) != getattr(state, field):
                    raise ValueError(
                        f"context builder removed or changed required field {field}"
                    )
            if request.candidates != candidates:
                raise ValueError("context builder changed candidate definitions")
            available = {o.id: o for o in request.state.observations}
            required = {o.id: o for o in required_view(state)}
            needed_ids = {oid for c in candidates for oid in c.required_observation_ids}
            required.update({o.id: o for o in state.observations if o.id in needed_ids})
            if any(available.get(oid) != value for oid, value in required.items()):
                raise ValueError("context builder removed required observations")
            if minimum_memory is not None:
                if request.memory is None:
                    raise ValueError("context builder removed cumulative memory")
                if (
                    any(
                        request.memory.progress.get(k) != v
                        for k, v in minimum_memory.progress.items()
                        if k != "omitted_facts"
                    )
                    or any(
                        f not in request.memory.facts for f in minimum_memory.facts[:2]
                    )
                    or any(
                        a not in request.memory.actions
                        for a in minimum_memory.actions[:1]
                    )
                ):
                    raise ValueError(
                        "context builder removed essential cumulative progress"
                    )
            reject_credentials(request.model_dump(mode="json"))

        # Duplicate IDs fail before entering the failure handler: do not alter old history.
        event = await store.ingest(state)
        evidence.add(f"{state.incident_id}:{event.sequence}")
        evidence.update(o.id for o in state.observations)
        try:
            for round_number in count(1):
                if remaining() <= 0:
                    return await terminate("incident deadline exhausted")
                await phase("observe")
                candidates = (
                    pending_recovery
                    if pending_recovery is not None
                    else await call(
                        registry.candidates,
                        state.model_copy(deep=True),
                    )
                )
                pending_recovery = None
                ids, allowed, rejected = set(), [], []
                for value in candidates:
                    candidate = ActionCandidate.model_validate(value).model_copy(
                        deep=True
                    )
                    if candidate.id in ids:
                        raise ValueError("duplicate candidate ID")
                    ids.add(candidate.id)
                    verdict = await gate(candidate, "before_selection")
                    if verdict.allowed:
                        allowed.append(candidate)
                    else:
                        rejected.append(verdict.reason)
                if not allowed:
                    return await terminate(
                        "no applicable actions"
                        + (
                            ": " + "; ".join(dict.fromkeys(rejected))
                            if rejected
                            else ""
                        )
                    )
                await phase("select")
                minimum_memory = None
                request = DecisionRequest.model_validate(
                    await call(
                        builder.build,
                        state.model_copy(deep=True),
                        [c.model_copy(deep=True) for c in allowed],
                    )
                )
                protected(request, allowed)
                minimum_memory = (
                    request.memory.model_copy(deep=True)
                    if request.memory is not None
                    else None
                )
                seen_contexts = set()
                while True:
                    serialized = request.model_dump_json()
                    if serialized in seen_contexts:
                        return await terminate(
                            "context builder made no progress reducing context"
                        )
                    seen_contexts.add(serialized)
                    try:
                        check = ContextCheck.model_validate(
                            await call(
                                provider.check_context, request.model_copy(deep=True)
                            )
                        )
                    except Exception as exc:
                        if isinstance(exc, TimeoutError) and remaining() <= 0:
                            raise
                        await record(
                            "provider_failure",
                            {
                                "stage": "context_check",
                                "error_type": type(exc).__name__,
                                "failure": failure_details(exc),
                            },
                        )
                        return await terminate("provider context check failed")
                    await record(
                        "context_check",
                        {"request_id": request.id, **check.model_dump(mode="json")},
                    )
                    if check.fits:
                        break
                    trimmed = await call(builder.trim, request.model_copy(deep=True))
                    if trimmed is None:
                        return await terminate(
                            "required incident context exceeds provider limits: "
                            + check.reason
                        )
                    trimmed = DecisionRequest.model_validate(trimmed)
                    protected(trimmed, allowed)
                    if len(trimmed.model_dump_json()) >= len(serialized):
                        return await terminate(
                            "context builder did not reduce request size"
                        )
                    request = trimmed
                await record(
                    "candidate_snapshot",
                    {
                        "request_id": request.id,
                        "candidates": [c.model_dump(mode="json") for c in allowed],
                    },
                )
                await record(
                    "provider_request",
                    {"round": round_number, "request": request.model_dump(mode="json")},
                )
                try:
                    capability_fn = getattr(provider, "capabilities", None)
                    capability_value = (
                        await call(capability_fn)
                        if capability_fn is not None
                        else DecisionCapabilities(max_decisions_per_round=1)
                    )
                    if isinstance(capability_value, DecisionCapabilities):
                        capability_value = capability_value.model_dump(
                            mode="json", warnings=False
                        )
                    capabilities = DecisionCapabilities.model_validate(capability_value)
                    reject_credentials(capabilities.model_dump(mode="json"))
                    await record(
                        "provider_capabilities",
                        {
                            "request_id": request.id,
                            **capabilities.model_dump(mode="json"),
                        },
                    )
                    response = await call(
                        provider.decide, request.model_copy(deep=True)
                    )
                    if isinstance(response, (DecisionResult, DecisionBatch)):
                        response = response.model_dump(mode="json", warnings=False)
                    response = (
                        DecisionBatch.model_validate(response)
                        if isinstance(response, dict) and "decisions" in response
                        else DecisionResult.model_validate(response)
                    )
                    reject_credentials(response.model_dump(mode="json"))
                    decisions = (
                        response.decisions
                        if isinstance(response, DecisionBatch)
                        else [response]
                    )
                    if len(decisions) > capabilities.max_decisions_per_round:
                        await record(
                            "provider_failure",
                            {"stage": "decision", "reason": "model capacity exceeded"},
                        )
                        return await terminate(
                            "provider exceeded its advertised model decision capacity"
                        )
                    if capabilities.metadata.get(
                        "allow_escalation", True
                    ) is False and any(d.operation == "escalate" for d in decisions):
                        raise ValueError(
                            "provider returned a disabled escalation operation"
                        )
                    supplied = {candidate.id for candidate in allowed}
                    if any(
                        decision.operation == "select"
                        and decision.candidate_id not in supplied
                        for decision in decisions
                    ):
                        raise ValueError("unknown candidate selection")
                except Exception as exc:
                    if isinstance(exc, TimeoutError) and remaining() <= 0:
                        raise
                    await record(
                        "provider_failure",
                        {
                            "stage": "decision",
                            "error_type": type(exc).__name__,
                            "failure": failure_details(exc),
                        },
                    )
                    return await terminate(
                        "provider failed or returned an invalid selection"
                    )
                await record(
                    "provider_result",
                    {
                        "request_id": request.id,
                        "result": response.model_dump(mode="json"),
                    },
                )
                for decision in decisions:
                    if decision.operation == "escalate":
                        return await terminate(
                            decision.reason or "provider requested escalation"
                        )
                    if decision.operation == "wait":
                        await phase("wait")
                        await record(
                            "wait_started",
                            {
                                "reason": decision.reason,
                                "seconds": decision.wait_seconds,
                            },
                        )
                        await asyncio.sleep(
                            max(0, min(decision.wait_seconds, remaining()))
                        )
                        break
                    selected = next(c for c in allowed if c.id == decision.candidate_id)
                    await phase("validate")
                    verdict = await gate(selected, "before_execution")
                    if not verdict.allowed:
                        return await terminate(
                            "selected action failed revalidation: " + verdict.reason
                        )
                    await phase("recover" if selected.kind == "recovery" else "execute")
                    if remaining() <= 0:
                        return await terminate(
                            "incident deadline exhausted before execution"
                        )
                    execution_id, started = new_id(), now()
                    # Commit start before calling any executor. Reserve uncertain changes too.
                    active = {
                        "execution_id": execution_id,
                        "action_id": selected.id,
                        "tool": selected.tool,
                        "target": selected.resources,
                        "started_at": started.isoformat(),
                    }
                    await record(
                        "execution_started",
                        {
                            **active,
                            "candidate": selected.model_dump(mode="json"),
                            "identity": identities[selected.id].model_dump(mode="json"),
                        },
                    )
                    attempts[identities[selected.id].key] = (
                        attempts.get(identities[selected.id].key, 0) + 1
                    )
                    changes += int(selected.effect == "change")
                    try:
                        transport = TransportResult.model_validate(
                            await call(
                                executor.execute,
                                selected.model_copy(deep=True),
                                state.model_copy(deep=True),
                                seconds=min(
                                    selected.timeout_seconds, self.limits.tool_seconds
                                ),
                            )
                        )
                    except TimeoutError:
                        await record(
                            "execution_unknown",
                            {
                                "execution_id": execution_id,
                                "reason": "tool or incident deadline exceeded",
                            },
                        )
                        await phase("execution_failed")
                        active = None
                        return await terminate(
                            "execution outcome unknown after timeout; reconciliation required"
                        )
                    raw_refs = []
                    step_outcomes = [
                        step.model_copy(deep=True) for step in transport.steps
                    ]
                    for step in step_outcomes:
                        if not set(step.target) <= set(selected.resources):
                            raise ValueError(
                                "procedure step target outside authorized scope"
                            )
                        if step.raw_output is not None:
                            step_ref = f"{state.incident_id}/{execution_id}/step/{step.step_id}"
                            await record(
                                "raw_output",
                                {
                                    "execution_id": execution_id,
                                    "step_id": step.step_id,
                                    "raw_output_refs": [step_ref],
                                },
                                {step_ref: step.raw_output},
                            )
                            step.raw_output = None
                            step.raw_output_refs = [step_ref]
                            raw_refs.append(step_ref)
                        elif step.raw_output_refs:
                            raise ValueError(
                                "plugins must supply step output, not unowned artifact references"
                            )
                    if transport.raw_output is not None:
                        raw_ref = f"{state.incident_id}/{execution_id}/raw"
                        await record(
                            "raw_output",
                            {
                                "execution_id": execution_id,
                                "raw_output_refs": [raw_ref],
                            },
                            {raw_ref: transport.raw_output},
                        )
                        raw_refs.append(raw_ref)
                    if transport.status != "succeeded":
                        parsed = ParseResult(
                            status="skipped",
                            parser_version="core/1",
                            reason="execution did not succeed",
                        )
                    elif transport.raw_output is None:
                        parsed = ParseResult(
                            status="missing",
                            parser_version="core/1",
                            reason="tool returned no output",
                        )
                    else:
                        try:
                            parsed = ParseResult.model_validate(
                                await call(
                                    registry.parse,
                                    selected.model_copy(deep=True),
                                    transport.model_copy(deep=True),
                                )
                            )
                            if any(
                                o.resource_id not in selected.resources
                                for o in parsed.observations
                            ) or any(
                                f.resource_id not in selected.resources
                                for f in parsed.memory_facts
                            ):
                                raise ValueError(
                                    "parsed observation outside action scope"
                                )
                            existing = {o.id for o in state.observations}
                            if any(
                                o.id in existing for o in parsed.observations
                            ) or len({o.id for o in parsed.observations}) != len(
                                parsed.observations
                            ):
                                raise ValueError("duplicate observation ID")
                            for observation in parsed.observations:
                                observation.evidence_refs = list(raw_refs)
                            for fact in parsed.memory_facts:
                                fact.evidence_refs = [
                                    *raw_refs,
                                    *(o.id for o in parsed.observations),
                                ]
                        except TimeoutError:
                            raise  # Incident deadline is authoritative even during parsing.
                        except Exception:
                            parsed = ParseResult(
                                status="failed",
                                parser_version="unknown",
                                reason="parser failed or returned invalid observations",
                            )
                    result = ExecutionResult(
                        execution_id=execution_id,
                        action_id=selected.id,
                        tool=selected.tool,
                        target=selected.resources,
                        started_at=started,
                        finished_at=now(),
                        status=transport.status,
                        parse=parsed,
                        transport_status=transport.transport_status,
                        command_exit_code=transport.command_exit_code,
                        raw_output_refs=raw_refs,
                        detail=transport.detail,
                        steps=step_outcomes,
                    )
                    # Execution records exclude parsed evidence; a separate event admits it.
                    await record("execution_result", result.model_dump(mode="json"))
                    state.attempts.append(result.model_dump(mode="json"))
                    active = None
                    await record(
                        "parse_outcome",
                        {
                            "execution_id": execution_id,
                            "parse": parsed.model_dump(mode="json"),
                        },
                    )
                    state.observations.extend(parsed.observations)
                    evidence.update(o.id for o in parsed.observations)
                    for step_number, step in enumerate(result.steps, 1):
                        await record(
                            "procedure_step",
                            {
                                "execution_id": execution_id,
                                "step_number": step_number,
                                "outcome": step.model_dump(mode="json"),
                            },
                        )
                    if result.status != "succeeded" or parsed.status != "valid":
                        await phase("execution_failed")
                    await phase("verify")
                    verification = VerificationResult.model_validate(
                        await call(
                            verifier.verify,
                            state.model_copy(deep=True),
                            selected.model_copy(deep=True),
                            result.model_copy(deep=True),
                        )
                    )
                    if verification.status == "passed":
                        if result.status != "succeeded" or parsed.status != "valid":
                            verification = VerificationResult(
                                status="inconclusive",
                                reason="execution lacks valid parsed evidence",
                            )
                        elif not set(verification.evidence_refs) <= evidence:
                            verification = VerificationResult(
                                status="inconclusive",
                                reason="verification referenced unknown evidence",
                            )
                        elif not set(verification.evidence_refs) & (
                            {o.id for o in parsed.observations} | set(raw_refs)
                        ):
                            verification = VerificationResult(
                                status="inconclusive",
                                reason="verification lacks evidence from current execution",
                            )
                    await record(
                        "verification",
                        {
                            "execution_id": execution_id,
                            **verification.model_dump(mode="json"),
                        },
                    )
                    if verification.status == "passed":
                        return await terminate(
                            verification.reason, "resolved", verification.evidence_refs
                        )
                    if result.status == "unknown":
                        return await terminate(
                            "execution outcome unknown; reconciliation required"
                        )
                    if (
                        result.status in {"failed", "partial"}
                        and selected.recovery
                        and selected.kind != "recovery"
                    ):
                        pending_recovery = await call(
                            registry.recover,
                            state.model_copy(deep=True),
                            selected.model_copy(deep=True),
                            result.model_copy(deep=True),
                        )
                    if result.status != "succeeded" or parsed.status != "valid":
                        break  # Refresh the model's view after execution/parsing failure.
        except TimeoutError:
            if active:
                await record(
                    "execution_unknown",
                    {
                        "execution_id": active["execution_id"],
                        "reason": "incident deadline exceeded",
                    },
                )
                await phase("execution_failed")
            return await terminate(
                "incident deadline exhausted"
                + ("; execution outcome unknown" if active else "")
            )
        except BaseException as exc:
            # Do not persist exception strings: third-party errors may contain credentials.
            if active:
                await record(
                    "execution_unknown",
                    {
                        "execution_id": active["execution_id"],
                        "reason": "runtime failure or interruption",
                    },
                )
            await phase("execution_failed")
            await record(
                "runtime_failure",
                {
                    "error_type": type(exc).__name__,
                    "failure": failure_details(exc),
                    "execution_id": active["execution_id"] if active else None,
                },
            )
            await terminate("runtime failure or interruption")
            raise


@factory(
    subsystem="incident_loop",
    component_type=DefaultIncidentLoop,
    settings_model=EmptySettings,
    dependencies=(
        "event_store",
        "tool_registry",
        "context_builder",
        "decision_provider",
        "validation_policy",
        "executor",
        "verifier",
    ),
)
def incident_loop(settings, context):
    return DefaultIncidentLoop(context)

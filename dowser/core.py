"""Default replaceable registry, bounded context, policy, executor, and verifier."""

from pydantic import BaseModel, Field

from .config import FactoryReference, check_component, load_factory
from .contracts import Component, ToolSpec, factory
from .memory import action_identity
from .models import (
    ActionCandidate,
    ActionIdentity,
    Boundary,
    DecisionRequest,
    InvestigationMemory,
    ValidationResult,
    VerificationResult,
    alert_value,
    mapped_value,
    now,
)
from .runtime import bounded_call
from .store import reject_credentials


class EmptySettings(Boundary):
    pass


class RegistrySettings(Boundary):
    plugins: list[FactoryReference] = Field(default_factory=list)


class DefaultRegistry(Component):
    def __init__(self, plugins):
        self.plugins = plugins
        self.tools: dict[str, tuple[ToolSpec, object]] = {}
        for plugin in plugins:
            check_component(type(plugin), "tool_plugin")
            for spec in plugin.tools:
                if not isinstance(spec, ToolSpec) or spec.name in self.tools:
                    raise ValueError("invalid or duplicate registered tool")
                if (
                    not issubclass(spec.argument_model, BaseModel)
                    or spec.argument_model.model_config.get("extra") != "forbid"
                    or spec.argument_model.model_config.get("strict") is not True
                ):
                    raise ValueError(
                        "tool argument schemas must be strict and forbid extras"
                    )
                if spec.effect not in {"read_only", "change"} or spec.kind not in {
                    "observation",
                    "remediation",
                    "recovery",
                }:
                    raise ValueError("invalid tool capability")
                if not spec.platforms or not spec.verification_hooks:
                    raise ValueError(
                        "tool must declare supported platforms and verification hooks"
                    )
                self.tools[spec.name] = (spec, plugin)

    def binding(self, candidate):
        if candidate.tool not in self.tools:
            raise ValueError("unsupported tool")
        spec, plugin = self.tools[candidate.tool]
        if (candidate.plugin_version, candidate.effect, candidate.kind) != (
            spec.plugin_version,
            spec.effect,
            spec.kind,
        ):
            raise ValueError("candidate contradicts registered tool capabilities")
        if candidate.verification not in spec.verification_hooks:
            raise ValueError("unregistered verification hook")
        if candidate.recovery and candidate.recovery not in spec.recovery_hooks:
            raise ValueError("unregistered recovery hook")
        reject_credentials(candidate.model_dump(mode="json"))
        spec.argument_model.model_validate(candidate.args)
        return spec, plugin

    def snapshot(self, candidates):
        result, ids = [], set()
        for value in candidates:
            candidate = ActionCandidate.model_validate(value).model_copy(deep=True)
            if candidate.id in ids:
                raise ValueError("duplicate candidate ID")
            ids.add(candidate.id)
            self.binding(candidate)
            result.append(candidate)
        return result

    async def candidates(self, state):
        result = []
        for plugin in self.plugins:
            result.extend(await plugin.candidates(state.model_copy(deep=True)))
        return self.snapshot(result)

    async def validate(self, candidate, state):
        try:
            spec, plugin = self.binding(candidate)
            scope = {r.id: r for r in state.resources}
            if len(set(candidate.resources)) != len(candidate.resources):
                return ValidationResult(
                    allowed=False, reason="duplicate affected resources"
                )
            for resource_id in candidate.resources:
                if resource_id not in scope:
                    return ValidationResult(
                        allowed=False, reason="target outside incident scope"
                    )
                resource = scope[resource_id]
                versions = spec.platforms.get(
                    resource.platform, spec.platforms.get("*", ())
                )
                if "*" not in versions and resource.platform_version not in versions:
                    return ValidationResult(
                        allowed=False, reason="unsupported platform/version"
                    )
            return ValidationResult.model_validate(
                await plugin.validate(
                    candidate.model_copy(deep=True), state.model_copy(deep=True)
                )
            )
        except (ValueError, KeyError):
            return ValidationResult(
                allowed=False,
                reason="invalid candidate or plugin precondition response",
            )

    async def execute(self, candidate, state):
        _, plugin = self.binding(candidate)
        return await plugin.execute(candidate, state)

    async def parse(self, candidate, result):
        _, plugin = self.binding(candidate)
        parsed = await plugin.parse(candidate, result)
        extract = getattr(plugin, "extract_memory", None)
        if extract is not None and parsed.status == "valid":
            parsed.memory_facts = await extract(candidate, parsed)
        return parsed

    async def action_identity(self, candidate, state):
        _, plugin = self.binding(candidate)
        hook = getattr(plugin, "action_identity", None)
        return (
            ActionIdentity.model_validate(await hook(candidate, state))
            if hook
            else action_identity(candidate)
        )

    async def verify(self, state, candidate, result):
        _, plugin = self.binding(candidate)
        return await plugin.verify(state, candidate, result)

    async def recover(self, state, candidate, result):
        _, plugin = self.binding(candidate)
        if not candidate.recovery:
            return []
        recovery = self.snapshot(await plugin.recover(state, candidate, result))
        if any(c.kind != "recovery" for c in recovery):
            raise ValueError("recovery hook must return registered recovery procedures")
        return recovery

    async def aclose(self):
        failures = []
        for plugin in reversed(self.plugins):
            try:
                await bounded_call(plugin.aclose, seconds=2)
            except BaseException as exc:
                failures.append(str(exc))
        if failures:
            raise RuntimeError("plugin cleanup failed: " + "; ".join(failures))


@factory(
    subsystem="tool_registry",
    component_type=DefaultRegistry,
    settings_model=RegistrySettings,
    dependencies=("event_store",),
)
async def tool_registry(settings, context):
    plugins = []
    try:
        for ref in settings.plugins:
            fn, config = load_factory(ref, "tool_plugin")
            plugin = fn(config, context)
            if hasattr(plugin, "__await__"):
                plugin = await plugin
            plugins.append(plugin)
            if not isinstance(plugin, fn.component_type):
                raise ValueError("plugin factory returned unexpected type")
        return DefaultRegistry(plugins)
    except BaseException:
        cleanup_errors = []
        for plugin in reversed(plugins):
            try:
                await bounded_call(plugin.aclose, seconds=2)
            except BaseException as exc:
                cleanup_errors.append(type(exc).__name__)
        if cleanup_errors:
            raise RuntimeError(
                "plugin startup and cleanup failed: " + "; ".join(cleanup_errors)
            )
        raise


class ContextSettings(Boundary):
    recent_outcomes: int = Field(default=4, ge=0)
    memory_bytes: int = Field(default=8192, ge=1024)


def required_view(state):
    latest = {}
    for observation in state.observations:
        key = (observation.resource_id, observation.kind)
        if key not in latest or latest[key].observed_at < observation.observed_at:
            latest[key] = observation
    return list(latest.values())


class DefaultContextBuilder(Component):
    def __init__(self, settings, store=None):
        self.settings = settings
        self.store = store

    async def build(self, state, candidates):
        view = state.model_copy(deep=True)
        view.observations = required_view(state)
        needed = {
            oid
            for candidate in candidates
            for oid in candidate.required_observation_ids
        }
        present = {o.id for o in view.observations}
        view.observations.extend(
            o for o in state.observations if o.id in needed - present
        )
        view.attempts = (
            state.attempts[-self.settings.recent_outcomes :]
            if self.settings.recent_outcomes
            else []
        )
        memory = None
        if self.store is not None:
            lookup = getattr(self.store, "memory", None)
            if lookup:
                memory = InvestigationMemory.model_validate(
                    await lookup(state, candidates)
                )
            else:
                # Legacy stores still supply cumulative outcomes via history.
                events = await self.store.history(state.incident_id)
                outcomes = [e for e in events if e.kind == "execution_result"]
                memory = InvestigationMemory(
                    actions=[
                        {
                            "event_ref": f"{e.incident_id}:{e.sequence}",
                            "tool": e.payload["tool"],
                            "status": e.payload["status"],
                        }
                        for e in outcomes[-8:]
                    ],
                    progress={"actions": len(outcomes)},
                )
            while len(memory.model_dump_json().encode()) > self.settings.memory_bytes:
                if len(memory.facts) > 2:
                    memory.facts.pop()
                    memory.progress["omitted_facts"] = (
                        int(memory.progress.get("omitted_facts", 0)) + 1
                    )
                elif len(memory.actions) > 1:
                    memory.actions.pop()
                else:
                    raise ValueError(
                        "essential cumulative memory exceeds context budget"
                    )
        return DecisionRequest(
            incident_id=state.incident_id,
            state=view,
            candidates=candidates,
            memory=memory,
        )

    async def trim(self, request):
        request = request.model_copy(deep=True)
        if request.state.attempts:
            request.state.attempts = request.state.attempts[1:]
            return request
        if request.memory is not None:
            if len(request.memory.facts) > 2:
                request.memory.facts.pop()
                request.memory.progress["omitted_facts"] = (
                    int(request.memory.progress.get("omitted_facts", 0)) + 1
                )
                return request
            if len(request.memory.actions) > 1:
                request.memory.actions.pop()
                return request
        return None


@factory(
    subsystem="context_builder",
    component_type=DefaultContextBuilder,
    settings_model=ContextSettings,
    dependencies=("event_store",),
)
def context_builder(settings, context):
    return DefaultContextBuilder(settings, context.require("event_store"))


class PolicySettings(Boundary):
    allow_changes: bool = False
    allowed_resources: list[str] | None = None
    severity_rules: dict[str, bool] | None = None
    priority_rules: dict[str, bool] | None = None


class DefaultPolicy(Component):
    def __init__(self, settings, context):
        self.settings = settings
        self.limits = context.limits

    async def validate(self, candidate, state, budget):
        if budget["seconds_remaining"] <= 0:
            return ValidationResult(allowed=False, reason="incident deadline exhausted")
        if budget["attempts"] >= self.limits.identical_attempts:
            return ValidationResult(
                allowed=False, reason="identical action attempt limit reached"
            )
        if candidate.effect == "change":
            if not self.settings.allow_changes:
                return ValidationResult(
                    allowed=False, reason="changes disabled by policy"
                )
            if budget["changes"] >= self.limits.changes:
                return ValidationResult(allowed=False, reason="change budget exhausted")
            for field in ("severity", "priority"):
                rules = getattr(self.settings, f"{field}_rules")
                if (
                    rules is not None
                    and mapped_value(rules, alert_value(state.alert, field), False)
                    is not True
                ):
                    return ValidationResult(
                        allowed=False, reason=f"changes blocked by {field} policy"
                    )
        if self.settings.allowed_resources is not None and not set(
            candidate.resources
        ) <= set(self.settings.allowed_resources):
            return ValidationResult(
                allowed=False, reason="target outside configured policy scope"
            )
        current = now()
        age = (current - candidate.created_at).total_seconds()
        if age < 0 or age > self.limits.freshness_seconds:
            return ValidationResult(
                allowed=False, reason="candidate is stale or future dated"
            )
        by_id = {o.id: o for o in state.observations}
        for oid in candidate.required_observation_ids:
            if oid not in by_id:
                return ValidationResult(
                    allowed=False, reason="required evidence missing"
                )
            observed = by_id[oid]
            age = (current - observed.observed_at).total_seconds()
            if (
                observed.resource_id not in candidate.resources
                or age < 0
                or age > self.limits.freshness_seconds
            ):
                return ValidationResult(
                    allowed=False,
                    reason="required evidence stale or outside action scope",
                )
        return ValidationResult(allowed=True)


@factory(
    subsystem="validation_policy",
    component_type=DefaultPolicy,
    settings_model=PolicySettings,
    dependencies=("tool_registry",),
)
def validation_policy(settings, context):
    return DefaultPolicy(settings, context)


class DefaultExecutor(Component):
    def __init__(self, context):
        self.registry = context.require("tool_registry")

    async def execute(self, candidate, state):
        return await self.registry.execute(candidate, state)


@factory(
    subsystem="executor",
    component_type=DefaultExecutor,
    settings_model=EmptySettings,
    dependencies=("tool_registry",),
)
def executor(settings, context):
    return DefaultExecutor(context)


class DefaultVerifier(Component):
    def __init__(self, context):
        self.registry = context.require("tool_registry")

    async def verify(self, state, candidate, result):
        if result.status != "succeeded" or result.parse.status != "valid":
            return VerificationResult(
                status="inconclusive", reason="execution lacks valid observations"
            )
        return await self.registry.verify(state, candidate, result)


@factory(
    subsystem="verifier",
    component_type=DefaultVerifier,
    settings_model=EmptySettings,
    dependencies=("tool_registry",),
)
def verifier(settings, context):
    return DefaultVerifier(context)

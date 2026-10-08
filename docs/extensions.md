# Extension contracts and configuration

## Factories

The eight execution slots remain mandatory in the version 1 JSON configuration.
Intake slots are optional; `serve` requires an `incident_source`. Each reference is
`{"factory": "module:callable", "settings": {...}}`. Settings default to `{}`.
Configuration rejects unknown fields. These are the provided factories:

| Slot | Default factory | Settings |
| --- | --- | --- |
| event_store | `dowser.store:sqlite_store` | `path` (default `.local/history.sqlite3`) |
| tool_registry | `dowser.core:tool_registry` | `plugins`: list of factory references |
| context_builder | `dowser.core:context_builder` | `recent_outcomes` (default 4), `memory_bytes` (default 8192) |
| decision_provider | `plugins.vllm:decision_provider`, `plugins.jev:decision_provider`, or your adapter | Deployment profile/endpoint settings or [Jev settings](jev.md) |
| validation_policy | `dowser.core:validation_policy` | `allow_changes` (false), `allowed_resources` (null), `severity_rules` / `priority_rules` (null) |
| executor | `dowser.core:executor` | Empty |
| verifier | `dowser.core:verifier` | Empty |
| incident_loop | `dowser.loop:incident_loop` | Empty |
| incident_source | `dowser.intake:jsonl_source` | `path`, `source_id` (default `jsonl`) |
| normalizer | `dowser.intake:compatibility_normalizer` | `severity_map`, `priority_map` (empty) |
| scheduler | `dowser.intake:fifo_scheduler` or `dowser.intake:mapped_rank_scheduler` | Empty for FIFO; `severity_ranks`, `priority_ranks` (empty) for mapped ranks |

An omitted normalizer or scheduler defaults to compatibility normalization and FIFO
when a source is configured. See [intake and checkpoints](intake.md) for `intake`
settings, extension examples, and source concurrency requirements. Old configurations
remain valid; `run` and `inspect` never construct intake services. Execution services
used by those commands must therefore have no dependency on an intake service.

`schema_version` defaults to `"1"`. `limits` defaults to:

```json
{
  "incident_seconds": 120,
  "tool_seconds": 15,
  "identical_attempts": 2,
  "changes": 0,
  "freshness_seconds": 60
}
```

The harness has no decision-round count limit. Model-specific decision volume is
advertised by the provider through the optional `capabilities()` contract below.
Legacy `limits.decision_rounds` entries are accepted and ignored; they are omitted
when configuration is serialized. The incident deadline and execution limits remain
independent of model capacity.

The `factory` decorator in `dowser.contracts` declares the subsystem,
interface version, component class, Pydantic settings class, and dependencies.
Factories accept `(settings, context)` positionally and may return a component
directly or awaitably. Illustrative declaration for an existing provider class:

```python
@factory(
    subsystem="decision_provider",
    component_type=YourProvider,
    settings_model=YourProviderSettings,
    dependencies=("event_store",),
)
def create_provider(settings, context):
    return YourProvider(settings, context.require("event_store"))
```

Use `Boundary` or a Pydantic model that rejects unknown settings. Components expose
`interface_version = "1"` and async methods matching their protocol in
`contracts.py`, including `aclose()`. Inheriting `Component` supplies the version
and a no-op close method. Runtime instances must match the declared component class.
Validation checks shape and signatures; it cannot prove implementation semantics.

`AppContext` provides a read-only mapping of **only declared** service dependencies,
a copy of global limits, and the working directory. Declare dependencies as a unique
tuple of slot names. Startup sorts the dependency graph and rejects missing services
and cycles. Shutdown closes successful constructions in reverse order, attempting
all closes even if one fails. Each close has a two-second deadline. Constructors
that fail before returning must release their own partially allocated resources.

The default registry loads `tool_plugin` factories in configured order and closes
them in reverse order. Its default service dependency set is empty. If a plugin
requires services, supply a registry factory declaring those dependencies; nested
plugin declarations must fit that dependency set. Custom registries own their nested
configuration validation and lifecycle.

## Public schemas and services

`models.py` contains the Pydantic JSON boundaries. Incident states, decision requests,
terminal results, and events are versioned. Provider adapters own encodings, tokenizer
selection, context accounting, and optional `score_metadata`; scores cannot authorize
execution or resolution.

| Interface | Methods, excluding `aclose` |
| --- | --- |
| EventStore | `ingest(state)`, `append(id, kind, payload, artifacts=None)`, `history(id)`, `reconstruct(id)`, `inspect(id)` |
| ToolRegistry / ToolPlugin | `candidates(state)`, `validate(candidate, state)`, `execute(candidate, state)`, `parse(candidate, transport)`, `verify(state, candidate, execution)`, `recover(state, candidate, execution)` |
| ContextBuilder | `build(state, candidates)`, `trim(request)` |
| DecisionProvider | `check_context(request)`, `decide(request)`; optional `capabilities()` |
| ValidationPolicy | `validate(candidate, state, budget)` |
| Executor | `execute(candidate, state)` |
| Verifier | `verify(state, candidate, execution)` |
| IncidentLoop | `run(state)` |
| IncidentSource | `open() -> AsyncIterator[RawIncident]`, `checkpoint(record, terminal_result)` |
| Normalizer | `normalize(raw_record) -> IncidentState \| None` |
| Scheduler | `select(pending_snapshots) -> str` (a supplied queue ID) |

`IncidentState` has alert and desired-state dictionaries, scoped `Resource` objects,
typed observations, attempts, instructions, unresolved questions, phase, and a
platform payload. The normalizer accepts the note's `recent_attempts` alias and
infers a resource from `alert.device_id` or `alert.resource_id` when needed. It preserves
other top-level fields in `payload` and alert-specific fields in the alert dictionary.
Fresh incidents must start in `observe` without prior attempts. Explicit resources
are preferred for platform/version and target scope. Resource IDs define the minimum
scope; plugins must also validate subresources such as interfaces against inventory.

`ActionCandidate` includes ID, registered tool/version, concrete arguments, factual
description, effect (`read_only` or `change`), kind (`observation`, `remediation`, or
`recovery`), resources, preconditions, required observation IDs, creation time,
timeout, and registered verification/recovery hook names. Use stable IDs and concrete
arguments; recovery hooks return candidate procedures, never executable model text.

## Tool plugins

A plugin class declares a nonempty tuple of `ToolSpec` objects. Each tool specifies
its name, plugin version, Pydantic argument model, effect, kind, supported platform
versions, verification hooks, and optional recovery hooks. Argument models must use
`ConfigDict(strict=True, extra="forbid")`. The registry checks arguments but passes
the original concrete JSON values to execution. Plugins should use JSON-native
argument types and validate target identifiers and allowed values in their schema.

Platform support maps names to version tuples; `"*"` explicitly opts into any
platform or version. A candidate cannot redefine a registered capability, tool
version, or hook. The registry rejects duplicate tool names and candidate IDs.

`validate` checks current external preconditions and correspondence between arguments
and declared resources, returning `ValidationResult`. It runs before selection and
immediately before execution. Textual `preconditions` document the checks; they
are not a generic expression language. The plugin implements every relevant check.
Plugins must declare required evidence IDs so the default policy can enforce scope
and freshness.

`execute` returns `TransportResult`, preserving execution status, transport status,
optional command exit code, sanitized raw output, and ordered `ProcedureStep` outcomes.
Each step includes identity, target, timing, status, parse outcome, optional raw output,
and partial-success details. The core stores step output as separate artifacts and
rejects steps outside the authorized scope or contradictory success reports.
A procedure owns its ordered checks and failure handling;
the alpha authorizes the whole declared procedure and counts it as one action/change.
It does not add a shell interface or automatically batch actions.

`parse` returns `ParseResult` with parser version and typed observations. Non-valid
parse results cannot contain observations. The core rejects out-of-scope or duplicate
observation IDs and attaches its persisted raw artifact references to observations.
Parsing never upgrades a failed/partial/unknown transport result into health evidence.

`verify` must check the original symptom, desired state, relevant service checks,
and any required persistence window. It returns passed, failed, or inconclusive.
Passed results require stored evidence references and at least one current observation
ID or current raw artifact reference. A hook name identifies the plugin procedure;
it does not prove that procedure's correctness. `recover` returns only registered
recovery candidates whose current preconditions can be revalidated.

## Context and provider adapters

`check_context` reports `ContextCheck(fits=..., reason=...)` using the adapter's
own accounting. It must not call a model. `decide` returns a normalized
`DecisionResult` or `DecisionBatch`. Each decision is `select` with a known candidate
ID, `wait` with a positive duration, or `escalate`. The core ships no substitute
provider and performs no tokenizer downloads.

Providers supporting multiple actionable choices expose
`async capabilities() -> DecisionCapabilities`. Its `max_decisions_per_round` is a
positive integer derived from the selected model's capabilities; there is no core
maximum. Optional JSON `metadata` can identify the model or capability version.
This method reports capabilities without invoking model inference. Its signature
is checked during constructor-free factory validation when present.

Metadata `allow_escalation: false` forbids a model-selected escalation response;
the loop rejects it before execution. Jev supports an optional provider-owned
`decision_policy` factory with `controls`, async `apply(request, decision,
alternatives)` and `aclose()`. This extension does not add a mandatory slot.
See [escalation controls and penalties](escalation.md).

One call to `decide` constitutes a model decision round. A provider advertising a
capacity of 64 may return up to 64 decisions in that response:

```python
async def capabilities(self):
    return DecisionCapabilities(max_decisions_per_round=self.model_decision_capacity)

async def decide(self, request):
    selected_ids = await self.model.select_candidates(request)
    return DecisionBatch(
        decisions=[
            DecisionResult(operation="select", candidate_id=candidate_id)
            for candidate_id in selected_ids
        ]
    )
```

The harness reads the provider's capacity for each round, records it, and validates
the entire response before executing any selection. Every selected ID must belong
to that request's authorized snapshot; exceeding the advertised capacity fails the
response. Batch decisions execute in order, one action at a time, with current
validation and verification after each action. Resolution ends the incident and
discards remaining selections. Execution or parsing failure discards the remaining
batch and refreshes the provider's view, offering recovery candidates when applicable.
`wait` or `escalate` may appear only as the final batch decision. A wait returns to
observation after its duration; escalation terminates the incident.

Existing single-result providers continue to work without `capabilities()`, using
their original one-decision response contract. To return more than one decision,
a provider must advertise its model's capacity. Provider-internal reasoning steps
and inference retries remain adapter responsibilities. There is no incident-wide
count limit on provider calls; the elapsed incident deadline still bounds them.

The default builder retains all alert/desired-state facts, scoped resources,
instructions, unresolved questions, platform payload, latest observations per
resource/kind, and required candidate evidence. It includes up to four recent action
outcomes. When the provider reports overflow, the oldest optional outcome is dropped
and checked again. Required facts and whole candidate definitions remain intact.
Every submitted request keeps its candidate snapshot and normalized result in storage.
Replacement builders are checked for preserving required facts and evidence; `trim`
must reduce serialized size and return `None` when optional history is exhausted.

## Runtime and trust

Factories and imported Python code are trusted. There is no sandbox, isolation,
hot reload, or security boundary between components. Default orchestration passes
deep copies of state/request/candidate boundaries to extensions.

Execution extension operations run serially in daemon worker threads, each with its
own asyncio loop, to let the owner enforce elapsed deadlines when an async method
blocks or ignores cancellation. Execution components must support calls across
threads/loops. Avoid retaining
loop-bound sessions or locks between calls; create them within a call, or own a
dedicated adapter transport thread. The SQLite implementation uses a thread lock.
Calls made directly by extension code are that extension's responsibility.

Each intake component has a persistent daemon worker loop. Its factory, asynchronous
construction, method calls, iterator pulls, and cleanup run on that loop. Intake
components may retain loop-owned clients and locks. Declared intake dependencies
are service proxies that route asynchronous calls to the dependency's owner loop.
Execution dependencies retain their existing runtime requirements. Sources must
support one outstanding iterator pull concurrently with checkpoint calls on their
loop; idle pulls have no deadline. Cleanup cancels outstanding tasks before calling
`aclose()` on the same loop, with the existing two-second cleanup deadline.

A timed-out worker may still affect the external system. The incident stops, records
an unknown outcome, and cannot be restarted under the same ID. Cleanup may overlap
an abandoned call. Use transport-level deadlines as well; reconciliation is required.
Daemon workers do not isolate native code holding the Python GIL or a crashed process.
These limits are deliberate alpha constraints, not a process sandbox guarantee.

Credentials belong in adapter-private settings/environment/credential storage, never
in incident facts, candidates, model requests, observations, or raw artifacts.
The core rejects common structured credential keys and avoids persisting arbitrary
exception text. Plugins/providers must sanitize free-text and raw output; key filtering
cannot discover every secret embedded in text. Configuration settings are never
persisted by the core. There is no live credential requirement for private validation.

## Bundled platform adapters

The installable `plugins` package provides NX-OS tools and normalization, a vLLM
decision provider, managed-vLLM tools and normalization, and a mixed-platform router.
See [platform configuration and verification](platforms.md) for factories, optional
dependencies, inventory scopes, tool catalogues and runnable JSON examples.

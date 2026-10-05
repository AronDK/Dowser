# Incident intake and scheduling

`dowser serve --config .local/config.json` runs this path:

```text
IncidentSource → Normalizer → bounded pending queue → Scheduler
                                                       ↓
                    source checkpoint ← durable terminal result ← IncidentLoop
```

Each process has one source, one normalizer, and one scheduler. Intake pulls and
normalizes while the incident loop processes previously queued work. Only one
incident executes at a time per node. Scheduling only chooses queued incidents;
new urgent records do not preempt the current incident. A finite source drains
the queue and exits. SIGINT/SIGTERM cancel intake while retaining the incident
loop's existing interruption handling.

## Configuration and bundled adapters

Add these fields to an existing eight-slot configuration:

```json
{
  "incident_source": {
    "factory": "dowser.intake:jsonl_source",
    "settings": {"path": ".local/incidents.jsonl", "source_id": "tenant/vendor"}
  },
  "normalizer": {
    "factory": "dowser.intake:compatibility_normalizer",
    "settings": {
      "severity_map": {"vendor-critical": "urgent", "vendor-warning": "routine"},
      "priority_map": {"1": "first", "2": "second"}
    }
  },
  "scheduler": {
    "factory": "dowser.intake:mapped_rank_scheduler",
    "settings": {
      "severity_ranks": {"urgent": 20, "routine": 10},
      "priority_ranks": {"first": 2, "second": 1}
    }
  },
  "intake": {
    "pending_capacity": 32,
    "normalization_seconds": 15,
    "scheduler_seconds": 15,
    "checkpoint_attempts": 3,
    "checkpoint_seconds": 5,
    "checkpoint_retry_delays": [0.5, 1.0]
  }
}
```

The source is required for `serve`. Omit `normalizer` to use compatibility
normalization with empty mappings; omit `scheduler` to use FIFO. The example
`intake` values are defaults. Capacity reserves space before pulling a record and
includes a record being normalized; the active incident is outside that capacity.
When capacity is exhausted, no further record is pulled. For additional checkpoint
attempts the final configured retry delay is reused. An empty delay list retries
immediately. Construction and source opening have 15-second deadlines.

The JSONL source opens a UTF-8 file and wraps each line's JSON value in a raw
envelope. `source_id` comes from settings; `event_id` is the one-based line number.
Its checkpoint is a no-op. Invalid JSON, blank lines, and file errors fail the
source stream and stop intake. A valid JSON value that cannot be normalized is
reported as an input failure and intake continues with subsequent lines. Keep
file identity and line ordering stable if IDs are derived from line numbers.

The compatibility normalizer wraps `normalize_incident()`, preserving native
alert details and unknown top-level fields in `payload`. An already namespaced
`incident_id` is preserved. A legacy ID is prefixed with `source_id:`. An omitted
ID uses `source_id:event_id`. This qualification applies to intake; the existing
`run --incident` normalization behavior stays unchanged. `IncidentState` remains
schema version 1.

`serve` emits one JSON terminal result per completed delivery, including duplicate
deliveries, on stdout. Diagnostics are JSON lines on stderr containing core-generated
categories and exception types, without adapter exception strings or raw records.
Checkpoint and reconciliation diagnostics identify the persisted incident ID.
A finite run exits 1 if an input failed, an existing incident needs reconciliation,
or checkpoint retries were exhausted. Explicit skips are successful and emit a
diagnostic without a result. Fatal source, normalization deadline, scheduler, and
execution failures also exit nonzero. Interruptions exit 130.

## Sources, normalization, and enrichment

Public protocols live in `dowser.contracts` and use the same versioned `@factory`
metadata, settings validation, dependency graph, and reverse cleanup as execution
extensions. Validation imports factory declarations and validates settings without
constructing components or opening a source.

`RawIncident` is a transient envelope:

```python
RawIncident(
    source_id="tenant/vendor",
    event_id="stable-platform-event-id",
    payload={"platform": "vendor", "alert": {}},
    metadata={"partition": "alerts"},  # optional JSON object
)
```

`async open()` returns an async iterator; it is a coroutine returning the iterator,
not an async generator method itself. `async checkpoint(record, result)` receives
the original raw record and the recorded `TerminalResult`. `async aclose()` releases
the iterator and source clients, including partially initialized resources.
Non-JSON checkpoint handles belong in source-private data keyed by stable event
identity. Raw records and metadata are never automatically persisted or sent to
the decision provider.

Normalizers asynchronously return `IncidentState` or `None`. Async normalization
can query inventory, scope related resources, enrich platform/version information,
and attach incident facts:

```python
class InventoryNormalizer(Component):
    def __init__(self, inventory):
        self.inventory = inventory

    async def normalize(self, record):
        if record.payload.get("ignore"):
            return None
        resources = await self.inventory.resources_for(record.payload["target"])
        return IncidentState(
            incident_id=f"{record.source_id}:{record.event_id}",
            alert=record.payload["alert"],
            desired_state=record.payload["desired_state"],
            resources=resources,
            payload={"inventory_revision": self.inventory.revision},
        )

    async def aclose(self):
        await self.inventory.aclose()
```

The harness revalidates every returned state, including custom normalizers and
mutated model instances. Fresh states require phase `observe`, no prior attempts,
nonempty unique resources, and observations within resource scope. Structured
credential fields are rejected. Normalizers must assign stable, globally namespaced
incident IDs in `namespace:identifier` form; the harness checks that both parts
are nonempty, while the plugin owns global uniqueness and stability. Random IDs
cannot support duplicate detection or checkpoint recovery. Normalization receives
a deep copy so it cannot change the original checkpoint record.

An explicit `None` or an ordinary normalization exception produces a sanitized
diagnostic and continues without checkpointing that record. A normalization worker
timeout stops intake. Plugins must sanitize facts and free text before returning
them; structured credential rejection cannot identify secrets embedded in text.

A composite source can merge several platforms into a single iterator, retaining
the originating platform and checkpoint handle privately. A composite normalizer
can route by `record.source_id` or a platform field. Such composites own their
nested clients, backpressure, routing, and cleanup and expose these same contracts.

## Severity, rank maps, and change policy

Native `alert.severity`, `alert.priority`, and all other platform fields are
preserved. Normalizers may supply `alert.canonical_severity` and
`alert.canonical_priority`; there is no required vocabulary. The compatibility
normalizer's maps add canonical values for mapped native values and preserve
already supplied canonical fields. Numeric native values use their string form
as JSON map keys.

FIFO selects the lowest arrival sequence. The mapped scheduler sorts by severity
rank descending, then priority rank descending, then arrival sequence ascending.
Each dimension uses its non-null canonical value when present, otherwise its
native value. Missing, structured, or unmapped values rank zero. An unmapped
canonical value does not fall back to a mapped native value. Configured ranks
may be negative. A custom scheduler receives a snapshot of `PendingIncident`
objects containing `queue_id`, `arrival_sequence`, and a deep copy of normalized
state. Return a `queue_id` from that snapshot. Selecting another ID, including
one that arrived while selection was running, fails intake. Checkpoint records
and source-private handles are excluded from the snapshot.

The default policy accepts optional `severity_rules` and `priority_rules` maps:

```json
{
  "allow_changes": true,
  "severity_rules": {"urgent": true, "routine": false},
  "priority_rules": {"first": true}
}
```

These rules restrict changes in addition to existing permission, resource-scope,
freshness, attempt, and change limits. They use the same canonical/native lookup
as scheduling. `null` disables a restriction; an empty map blocks all changes
for that dimension. When a restriction is enabled, missing or unmapped values
deny changes. Every enabled dimension must explicitly map the incident value
to `true`. Read-only actions remain available. Supply a different
`validation_policy` factory to replace these policy choices; the incident loop's
scope and execution budgets still apply.

## Durable completion and redelivery

Checkpointing means **processing completed**, for all outcomes: `resolved`,
`escalated`, and `recovery_unverified`. Plugins decide whether that means
acknowledging a platform alert, updating a ticket, or advancing a cursor. It does
not imply the underlying symptom was resolved.

The harness reads a durable `terminated` event from the existing event store
before checkpointing. Custom incident loops must commit this event with the
`TerminalResult` payload before returning the same result. No store contract or
SQLite schema migration is required. A phase transition alone is insufficient.
If execution raises after recording termination, intake attempts checkpointing
before propagating the runtime failure.

By default, checkpointing tries three times, with five seconds per attempt and
0.5-second then 1-second retry delays. Each completed attempt appends a sanitized
`checkpoint_attempt` event with its attempt number, status, and failure type when
applicable. An interrupted attempt records status `interrupted` before propagating
cancellation. Exhausted retries mark the run unsuccessful and allow subsequent
incidents to proceed. A checkpoint timeout may leave external effects uncertain;
all source checkpoint operations must be idempotent, including overlapping retries
if a timed-out call ignores cancellation.

On redelivery, an existing durable terminal result is emitted and checkpointed
again without executing actions. An existing incident without a terminal event
stays uncheckpointed and is reported for reconciliation. A crash after termination
but before checkpointing is therefore recoverable through source redelivery. A
restart does not automatically retry checkpoints or resume incomplete execution.

Mapped scheduling can complete records out of source order. Checkpoints must
tolerate out-of-order completion. Cursor-based sources must track completed records
and advance only across a contiguous completed prefix; checkpointing a later
record cannot skip an earlier failed, skipped, pending, or incomplete record.
Sources must own any durability needed for that tracking, including redelivery
after restart. There is no active-incident merging or distributed coordination.

## Runtime ownership

Each intake component owns a persistent daemon worker event loop for construction,
calls, and cleanup. Iterators, sessions, and locks can retain their loop ownership.
One iterator pull may be outstanding while checkpoints execute on the source's
loop. A source must support that concurrency and must yield control during idle
waiting so checkpoints can proceed. Idle pulls have no elapsed deadline.
Normalization and scheduler calls have independent deadlines and loops, so they
can enrich and select without blocking incident execution.

Execution extensions retain their existing per-call worker runtime. An intake
component declaring an intake dependency receives a proxy routing its async calls
to the dependency's loop; declaring an execution dependency does not change that
service's ownership contract. Composite plugins own their nested lifecycle.

Shutdown cancels intake and outstanding worker tasks and then calls `aclose()` on
each component's owner loop, in reverse dependency order. Cleanup has the existing
two-second deadline. A blocking or uncooperative worker can outlive a timeout and
continue external work; worker threads are not isolation. Use transport deadlines
and idempotency within plugins as well.

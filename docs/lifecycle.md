# Lifecycle and durable history

The default loop processes one incident at a time. It uses a monotonic incident
deadline, separate from timestamps in evidence. It constructs registered candidates,
filters them through registry validation and policy, records a bounded request, and
normalizes the provider's response against that exact candidate snapshot.

## Phases and stopping rules

| Phase | Default behavior |
| --- | --- |
| observe | Gather concrete candidates from enabled plugins |
| select | Bound context and obtain a normalized decision |
| validate | Revalidate selected arguments, scope, capabilities, freshness, preconditions, policy, and budgets |
| execute | Commit an execution start, then invoke one registered action |
| verify | Evaluate incident-specific evidence after execution |
| wait | Wait for the provider's defined reason, bounded by the incident deadline |
| execution_failed | Record failed, partial, unknown, interrupted, or unparsed outcomes |
| recover | Execute a selected, currently validated recovery procedure |
| resolved | Affirmative verification passed with current stored evidence |
| recovery_unverified | A change was attempted and affirmative recovery evidence is unavailable |
| escalated | Actions, evidence, context, permissions, or budgets cannot support continuation |

After each completed action the verifier runs. Failed or inconclusive verification
returns to observation unless the action outcome is unknown. A registered recovery
hook is considered after a failed or partial action; it must return applicable
recovery candidates. Those candidates still go through selection and both validation
stages. No hook, no applicable procedure, changed preconditions, or exhausted budget
prevents recovery. The loop does not roll back every failure or automatically retry
an unknown operation.

The default policy enables reads. Changes require explicit policy permission and a
positive remaining change budget. Attempt accounting keys on canonical tool name
and arguments, regardless of candidate ID. Change attempts reserve budget before
execution even if the outcome later fails or becomes unknown. A plugin-defined
procedure counts as one declared change; its steps have individual recorded outcomes.
Observation, remediation, and recovery all pass the same gate.

Each decision round includes candidate construction, context checking, one decision,
and an optional action or wait. Limits prevent endless ineffective observation,
failed changes, model calls, and waiting. The action timeout is the minimum of its
declared timeout, the global tool timeout, and the remaining incident duration.
Provider and other extension calls are bounded by the remaining incident duration.
If required context cannot fit, the incident escalates with an explicit reason before
calling `decide`. Invalid selections and provider errors produce recorded failures.

The terminal result includes an outcome, reason, incident ID, and stored evidence
references. If any change was started and resolution lacks evidence, the default
terminal outcome is `recovery_unverified`; otherwise it is `escalated`. Runtime
exceptions are recorded, terminate the incident, and propagate for a nonzero CLI exit.
The loop can record failures only while its event store remains available.

## Execution and parsing

The execution-start transaction commits before a tool is invoked. Successful command
execution and valid parsing are separate conditions. Missing output, parse exceptions,
malformed observations, failed commands, partial procedures, and unknown transports
cannot produce healthy observations. Raw output is stored before parsing. Parsing
results have a separate event and parser version.

Verification cannot resolve an action with a non-successful transport or invalid
parse status. Evidence references must exist in the current incident history, and
at least one must refer to the current action's parsed observation or raw artifact.
Incident-specific semantics and persistence periods are implemented by verifier/plugin
contracts; the core cannot prove that external evidence actually demonstrates recovery.

## SQLite

The default store creates `incidents`, `events`, and `artifacts`. Events carry schema
version, incident ID, monotonically increasing sequence, UTC timestamp, kind, and
JSON payload. Inserts use serialized writes and `BEGIN IMMEDIATE`; event and associated
artifact inserts commit atomically. Foreign keys link artifacts to their owning event.
Raw artifact references are checked against the same incident. SQLite triggers prohibit
event/artifact updates and deletes through the store connection.

Recorded event kinds include ingestion, state transitions, validation, context checks,
candidate snapshots, provider requests/results/failures, execution starts/results,
unknown outcomes, raw output, parse outcomes, procedure steps, verification, waits,
runtime failures, and termination. Event references are `INCIDENT_ID:SEQUENCE`; raw
references are `INCIDENT_ID/EXECUTION_ID/raw`; observations also have stable IDs.

Reconstruction applies ingestion, phase transitions, completed execution results,
and separate parse outcomes. An execution start with no completed result contributes
an unknown attempt. Inspection preserves full events/artifacts and marks those
unfinished executions as unknown. Hard interruption can leave the last phase as
`execute`; inspection still identifies the unknown operation.

Existing incident IDs are rejected without modifying their history. SIGINT/SIGTERM
are handled where supported, preserving failure events and closing components.
SIGKILL cannot record a termination, but the committed start remains durable.
No automatic resumption, command replay, restart reconciliation, history compaction,
retention management, or database migration is supplied. Perform external reconciliation
before creating any separately authorized follow-up incident.

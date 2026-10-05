# Core alpha handoff

Implemented in the Dowser repository on 2026-10-02 from the
provided development plan and the current `System One Harness.md` note. The repository
started empty. No deployment or package-registry publication is included.

The core includes the Python package/CLI, all eight configurable execution slots,
declarative factory compatibility checks, dependency ordering and reverse cleanup,
default registered-tool execution and policy, bounded context, incident lifecycle,
verification, elapsed deadlines, and transactional SQLite events/artifacts. A decision
provider is required and has no default. Tool plugins are supplied by deployments.

On 2026-10-05, the pluggable intake plan added three optional factory slots:
`incident_source`, `normalizer`, and `scheduler`. `serve` uses persistent intake
worker loops, a bounded queue, serial incident scheduling, and source checkpoints
after durable terminal outcomes. Bundled adapters include JSONL, compatibility
normalization with optional severity/priority mappings, FIFO, and mapped-rank
scheduling. The default policy supports optional severity and priority restrictions.
Existing commands/configurations and incident/store schema version 1 are preserved.
See the [intake guide](intake.md) for configuration and lifecycle requirements.

## Verification

Private tests and fixtures are under `.local/tests/` and `.local/fixtures/`; `.gitignore`
excludes them along with databases, logs, environments, caches, and build artifacts.
They use the same factory declarations and public schemas as future extensions.
No double, sample incident, public demo, or CI workflow is part of the tracked source.

Run the suite locally after `uv sync --locked`:

```sh
uv run python -m unittest discover -s .local/tests -v
```

Acceptance result: **93 tests passed** (49 original and 44 intake tests), including
parameterized subsystem replacement
and failure cases. Full output is in `.local/test-results.txt`. The suite blocks socket
connections and runs CLI subprocesses with no credentials. It uses no models, model
weights, paid inference, devices, or infrastructure access.

Verified cases include:

- All eight subsystem replacements, factory order/signatures/version checks, missing
  dependencies, cycles, invalid settings, and cleanup during component/plugin startup
  failures, including continuation after a close failure.
- Observation, explicitly enabled simulated change, incident-specific verification,
  resolution, reopened SQLite history, and consistent artifact references.
- No actions, provider/context-check failure, malformed/unknown selection, explicit
  escalation, required context overflow, and optional history trimming.
- Revalidation after preconditions change; policy, scope, platform/version, freshness,
  required evidence, registered capability, and concrete argument enforcement.
- Missing/malformed output, parser failure, inconclusive/fabricated verification,
  ineffective repetition, and round, duration, tool, candidate, attempt, and change limits.
- Partial procedures, separate per-step artifacts/outcomes, registered recovery,
  recovery budget rejection, changed recovery preconditions, and no forced rollback.
- Blocking/uncooperative tools and providers, cancellation, SIGINT, SIGTERM, SIGKILL,
  unknown executions on inspection, nonzero runtime failures, and rejected duplicate IDs.
- Event ordering, append-only protection, transactional rollback on artifact collisions
  or unknown references, state reconstruction, and credential-field rejection.
- CLI validation without construction and inspection without constructing unrelated
  providers/plugins.
- Persistent intake construction, iterator/client ownership, dependency proxies,
  cleanup after startup failures, bounded pending capacity, idle cancellation,
  finite exhaustion, and serial nonpreemptive scheduling.
- Async enrichment, explicit skips, invalid/mutated normalized states, credential
  rejection, platform-field preservation, canonical mappings, FIFO and rank order,
  stable ties, snapshot-only selections, and severity/priority policy restrictions.
- All terminal checkpoints, bounded retries/timeouts, continued execution after
  checkpoint failure, duplicate delivery without action replay, incomplete-incident
  reconciliation, execution failure/interruption after durable termination, and
  SIGKILL between termination and checkpoint followed by restart/redelivery.
- JSONL stdout, sanitized stderr, finite-run exit status, missing source, SIGINT/
  SIGTERM idle interruption, and `run`/`inspect` avoiding intake construction.

The package builds as a source distribution and a wheel using `uv build --out-dir
.local/dist`. Ruff lint/format checks, source compilation, and whitespace checks pass.
Built package contents were inspected to ensure private doubles and runtime artifacts
are excluded. Validation used Python 3.14.7, uv 0.12.5, and Pydantic 2.13.5.

## Limits of this verification

These results establish harness mechanics against offline doubles. They provide no
evidence of model decision quality, real-device safety/compatibility, remediation
effectiveness, production latency, or operational benchmarks. Verification hooks
remain trusted code responsible for incident-specific evidence and persistence windows.

Execution extension operations run across per-call daemon worker threads/asyncio
loops; intake adapters have persistent owner loops. Adapters must respect those
lifecycles. Deadlines stop orchestration and record unknown effects, but
cannot undo external work, isolate a native extension holding the GIL, or recover a
killed process. See [extension runtime requirements](extensions.md#runtime-and-trust).
There is no automatic execution resumption, command replay, plugin isolation,
active-incident merging, distributed coordination, NXOS plugin, or concrete Jev/CLM
adapter. Checkpoint retry after restart depends on source redelivery; there is no
automatic scan or checkpoint retry job.

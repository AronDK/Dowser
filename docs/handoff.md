# Core alpha handoff

Implemented in the Dowser repository on 2026-10-02 from the
provided development plan and the current `System One Harness.md` note. The repository
started empty. No deployment or package-registry publication is included.

The core includes the Python package/CLI, all eight configurable execution slots,
declarative factory compatibility checks, dependency ordering and reverse cleanup,
default registered-tool execution and policy, bounded context, incident lifecycle,
verification, elapsed deadlines, and transactional SQLite events/artifacts. A decision
provider is required and has no default. Deployments select bundled or custom tool plugins.

On 2026-10-05, the pluggable intake plan added three optional factory slots:
`incident_source`, `normalizer`, and `scheduler`. `serve` uses persistent intake
worker loops, a bounded queue, serial incident scheduling, and source checkpoints
after durable terminal outcomes. Bundled adapters include JSONL, compatibility
normalization with optional severity/priority mappings, FIFO, and mapped-rank
scheduling. The default policy supports optional severity and priority restrictions.
Existing commands/configurations and incident/store schema version 1 are preserved.
See the [intake guide](intake.md) for configuration and lifecycle requirements.

On 2026-10-06, the core package moved from `src/dowser/` to `dowser/`, with a
`plugins/` directory reserved for platform extensions. The fixed incident-wide
decision-round count was removed. Providers may expose model-specific
`DecisionCapabilities` and return an ordered `DecisionBatch`, with selections
validated against the advertised capacity and executed serially through the
existing policy and verification gates. Legacy single-result providers remain
compatible; old `limits.decision_rounds` settings are accepted and ignored.

## Verification

Private tests and fixtures are under `.local/tests/` and `.local/fixtures/`; `.gitignore`
excludes them along with databases, logs, environments, caches, and build artifacts.
They use the same factory declarations and public schemas as future extensions.
No double, sample incident, public demo, or CI workflow is part of the tracked source.

Run the suite locally after `uv sync --locked`:

```sh
uv run python -m unittest discover -s .local/tests -v
```

Acceptance result: **116 tests passed** (93 existing and 23 decision-capability tests), including
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
  ineffective repetition, duration, tool, candidate, attempt, and change limits.
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
- Model-specific capacities of 1, 2, 32, and 64, with no fixed core maximum;
  more than ten rounds per incident; legacy single-result and configuration
  compatibility; constructor-free capability-signature validation.
- Whole-batch validation before effects, capacity rejection, serial execution,
  revalidation after each effect, early resolution, failure-driven context refresh,
  registered recovery, final wait/escalation, and preserved execution budgets.

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
active-incident merging, distributed coordination, or concrete Jev/CLM adapter. Checkpoint retry after restart depends on source redelivery; there is no
automatic scan or checkpoint retry job.

## Platform adapters (2026-10-06)

The installable `plugins` package now includes NX-OS 10.4(x) tools over verified
NX-API HTTPS and pinned-host-key SSH, both vLLM roles (SOM decisions and managed
Docker/Compose services), private inventory models, and explicit-platform JSONL
normalization. See [the platform guide](platforms.md) and its read-only and opt-in
remediation examples. Changes require plugin enablement, policy permission and an
explicit budget. Unknown writes terminate for reconciliation. A SOM outage escalates
without inference failover or unsolicited remediation.

Verification: 44 tracked offline platform tests and all 116 existing private harness
tests passed (160 total). Platform tests are in `tests/test_platforms.py`; they block
socket connections and use simulated transports. Their output is recorded in
`.local/platform-test-results.txt`; regression output is in
`.local/platform-core-regression-results.txt`. The wheel and source distribution
build successfully, exclude private/runtime artifacts, and the wheel imports and
validates factories in a fresh base-only environment without HTTPX/AsyncSSH/Prometheus.
Lint, format, compilation, whitespace and both example configuration checks pass.

These checks cover schema/scope enforcement, safe runtime ownership, procedure
outcomes, readiness, performance windows and harness integration. NX-OS fixtures
are simulated structured responses; no real-device, GPU, model-quality, deployment,
or performance benchmark claims follow. Optional read-only lab checks were not run.

## ITBench-AA evaluation (2026-10-07)

The optional native Jev adapter and `dowser-bench` runner were merged from the
earlier worktree into the main checkout, preserving the existing platform adapters.
The runtime worker wake-up fix and its regression tests are included. See the
[Jev guide](jev.md) and [evaluation guide](itbench-aa.md).

Verification passed 83 tracked tests and all 116 private core tests. The updated
26-test benchmark suite, including the simulated ten-pilot plus 120-trial campaign
and immediate pilot halt on provider failure, also passed. A private signal test
now waits for CLI initialization instead of relying on a fixed startup delay.
Lint, formatting, lock validation, compilation, package builds and package-content
checks passed.

All 3,799 files at revision `76df38a82288f75ba9e41dc8c515033332497473`, all
40 snapshot indexes, and all 40 initial contexts were verified. The live campaign
`jev-1130-20261006` started, but pilot scenario 8 escalated after call 8 raised
`JevError`. The evaluation is incomplete: one pilot trial terminated and no
full-run trial started. There is no full-run score. Seven calls have known usage;
one retains its full reservation. Accounted spending is $0.004398198 of $20.

The failed request passes offline schema, candidate-count, context-size and
credential-exclusion checks. The existing adapter records only the exception type;
the precise HTTP, transport or invalid-response cause cannot be recovered from
the stored artifacts. The failed trial was preserved without inference retries,
fallback or replay. Further trials require resolving the provider failure and
explicitly recording any changed implementation or replacement-pilot provenance.

The frozen manifest, pricing source, complete preparation status, preflight,
history, ledger, scoring, JSONL/CSV results, `summary.md`, `execution-status.md`
and `failure-analysis.json` are under `.local/itbench-aa/jev-1130-20261006/`.
The earlier worktree and its original artifacts remain intact.

The user subsequently authorized persistent structured diagnostics, bounded
HTTP 429/529 retries with TypeSafe-recommended exponential backoff, and a rerun.
The total spending ceiling is now $15, including previous accounted spending.
The original source is archived under the first campaign's `source/` directory.
A new campaign carries its frozen spending snapshot forward and retains the
original failed trial. See the updated evaluation and Jev guides.

## Cumulative memory correction — 2026-10-07

The original SQLite event log existed, but the ITBench context builder explicitly
removed all attempts and retained only the latest investigation observation.
Revision-bearing arguments defeated exact-argument repetition checks. Existing
tests validated storage and simulated execution, not actual model use of history.
The completed SDK campaign recorded 120 full trials (2/120 exact accuracy), with
106 call-limit failures, 7 submissions and 7 provider failures; cumulative accounted
cost was $3.379047336. Those artifacts remain unchanged.

The user authorized cumulative SQLite memory, semantic identities, recall, scoped
related-alert retrieval and real Jev validation. They explicitly removed the $15
ceiling. New campaigns have no spending or call-count ceiling; accounting and the
80 requests/s and 100,000 tokens/s hard caps remain. SOM controls decisions; a
30-minute watchdog reports incomplete execution. See [memory behavior and
validation](memory.md). The authorized live scope is focused paired tests and ten
pilot cases, without replay or a new full campaign. ALFworld is subsequent work.

Real Jev memory validation passed 30/30 paired decisions; memory-omitted controls
scored 9/15 (45 actual HTTP calls total). The ten-case pilot is supervised under
`.local/itbench-aa/jev-memory-20261007-native/`. Its first case ended by model
escalation after 11 calls and zero repeated evidence reads. A reporting-only
classification fix corrected successful context trimming being treated as fatal;
the first paid trial remains untouched and is not replayed. The original source
and approved reporting revision are archived. Read current local supervisor
status for final results; the model/loop/plugin execution files stay frozen.

## User stop and publication — 2026-10-07

The user explicitly ended testing. The pilot process was interrupted and its
completion notifier cancelled; no paid trial was restarted or replayed.
Four pilot cases finished: two model escalations (scenarios 8 and 2), one correct
submission (scenario 19), and one provider deadline failure (scenario 17). Exact
accuracy across these four cases and their mean score are both 25% (1/4).
Scenario 16 was interrupted during investigation and is a partial fifth case,
excluded from that four-case accuracy. The ten-case pilot remains incomplete.

Real Jev memory validation remains 30/30 correct, with 9/15 controls. The refreshed
local ledger includes 1299 pilot HTTP attempts, including the partial fifth case,
and cumulative accounted cost of $4.112606652 including earlier campaigns and
unknown reservations. Reports/history/ledger stay in the gitignored local run
directory. Testing is paused: do not auto-resume or start full ITBench/ALFworld.

The user authorized committing and pushing the implemented Jev provider, retries,
structured diagnostics, rate limiting, accounting, ITBench tools/runner, SQLite
cumulative memory, semantic repetition/recall, tests and documentation. Existing
verification and real-model validation are recorded above; no regression suites
were repeated during this stop/publication task. Credentials, datasets and runtime
artifacts remain local.

# Decisions, repeat execution and SQLite retrieval

Dowser supports the official Python SDK's native `POST /v1/decisions` endpoint with
`gpt-6-luna`. The harness supplies the alert, instructions, complete action
catalogue, admitted findings and prior outcomes. The model selects an action;
Dowser validates, executes, persists and verifies it. Previous use does not remove
an authorized action. `limits.identical_attempts` still loads for compatibility
but no longer vetoes execution. Effect budgets, scope, strict arguments,
credentials, preconditions and reconciliation of unknown mutations still apply.

The [official Decisions guide](https://developers.openai.com/api/docs/guides/decisions)
describes native choice, predicate and score answers. The
[reference](https://developers.openai.com/api/reference/resources/decisions/methods/create)
permits 2–255 choices; the [model page](https://developers.openai.com/api/docs/models/gpt-6-luna)
lists a 1,050,000-token context. Dowser reserves framing capacity and defaults to
1,000,000 estimated input tokens including questions and the catalogue.

Install with `uv sync --locked --extra openai --extra itbench-aa`. Supply
`OPENAI_API_KEY` in the environment or `.env`. Credentials do not enter prompts or
journals. SDK retries are disabled; Dowser owns admission, retries, deadlines and
billing reservations. Configure the actual account's `requests_per_minute` and
`tokens_per_minute` before any paid call. No account limits are guessed, and no
monetary or model-call ceiling is introduced.

A generic application's provider and context configuration can use:

```json
{
  "decision_provider": {
    "factory": "plugins.openai_decisions:decision_provider",
    "settings": {
      "requests_per_minute": 60,
      "tokens_per_minute": 2000000
    }
  },
  "context_builder": {
    "factory": "dowser.core:context_builder",
    "settings": {"profile": "large"}
  }
}
```

Those example account values must be replaced with the account's limits. Register
`plugins.history:tool_plugin` in the existing registry to offer read-only history
retrieval. The benchmark does this automatically. SQLite remains authoritative.

## Selection and context

Menus are sorted deterministically by action ID, partitioned into at most 254
actions plus wait, and evaluated in batches of at most six independent questions.
Batch packing also respects configured TPM admission; it sends fewer questions
when the shared input plus six questions would exceed that allowance.
Every evaluation receives the same complete catalogue and shared evidence.
Repeated per-tool fields, text prefixes, repeated argument strings, description
templates and identical history lookup results are encoded losslessly with explicit
defaults and tables; decoding restores every candidate field and argument.
The current `per_tool_tables/2` encoding supplies an ordered `candidate_tables`
array. Each table contains `tool`, `columns` and `rows`. A row is
`[mask, ...values]`: bit `2^i` selects `columns[i]`, and values follow set bits in
column order. Unset bits mean absent fields, preserving the distinction from
explicit null. The table supplies the tool; existing defaults, prefixes, string
tables and description templates restore other fields. Optional `candidate_order`
indexes the flattened rows to preserve the original order across mixed tools.
JSON object-key reordering does not change decoding. The full plain catalogue is
removed from the compressed input; complete proposed definitions remain in SQLite
and the audit. The decoder still accepts saved `per_tool_defaults/1` inputs.

Native choice values always use the actual supplied action IDs. A new
request selects among group winners and wait; this repeats if necessary. If all
groups choose wait, that wait is returned directly. Native choices are preserved;
local distributions are never compared as global probabilities. Refusals,
unknown labels and malformed answers fail explicitly. There is no provider
fallback. Hypotheses, scores and generated diagnosis text are interpretations,
not private model reasoning or execution permissions.

The large profile reads SQLite projections before copying context. Recent choices
use compact indexed projections; full native score matrices stay in the audit. It includes
all admitted facts when they fit, the action/outcome ledger, recent model choices
including waits, and relevant full saved results. Unchanged values are deduplicated;
contradictions remain separate. Original segments, provenance, uncertainty and
incomplete-page markers survive. Overflow preserves required state and the whole
catalogue, prioritizes contradictions and alert/focus evidence, and publishes
omission counts and retrieval queries/references.

The effective per-request budget is the smaller of the configured context budget
and TPM allowance. Overflow may omit optional evidence with retrieval references;
the complete catalogue remains required. Pilot preflight checks the ten requested
cases; full-campaign preflight checks every prepared snapshot. A snapshot fitting
the model context can still exceed a smaller account admission allowance.

Token accounting is an estimate, never an exact count inferred from bytes.
Locally installed tokenizer assets are used when available, with the tokenizer
name recorded. Unrecognized models use an explicitly labelled `o200k_base`
estimate. Without local assets, a conservative UTF-8 upper bound is labelled as
such; context checks never download assets. Install/cache tokenizer assets before
large runs to avoid needlessly conservative admission. Estimates and reported
usage are recorded separately.

## History and freshness

Optional store methods `action_history(state, identities)` and
`query_history(state, query)` maintain compatibility with existing adapters.
Candidate history reports successful empty lookup, lookup failure, prior failure,
unknown execution and rejection separately. Matching ignores candidate UUIDs and
benchmark view revisions. Evidence versions, original times, result references
and older versions remain explicit.

`history.query` accepts fixed data fields: `category`, `text`, `entity`, `tool`,
`outcome`, `evidence_kind`, `after`, `before`, `artifact_ref`, `offset`, and `limit`
(1–200). Queries use parameterized SQLite filters and quoted FTS5 terms. There is
no SQL argument, arbitrary filesystem access or grader retrieval. Concrete
pagination, filter and artifact options are derived from admitted results. Action
and artifact queries are isolated to the current incident/trial; related-incident
fact memory continues to require equal trusted scopes.

Retrieving saved history does not admit its contents as a fresh observation.
Live evidence keeps its original observation time and requires revalidation when
stale. Benchmark observations explicitly carry their immutable snapshot fingerprint;
the benchmark policy accepts them throughout the watchdog period only while that
fingerprint and incident scope match. Global navigation always offers every
observed entity, browser page and eligible evidence inspection, regardless of view.
Fresh inspections remain available alongside explicit recall/history actions.

## Audit and accounting

Benchmark runs automatically mirror to `audit.jsonl`, with hashed large payloads
and tool output files in `audit.jsonl.artifacts/`. Each record contains incident
and sequence identifiers and a hash chain. API events identify provider, model,
request, attempt and grouping stage; native scores and label mappings are saved.
Execution starts commit before effects; raw output commits before parsing. SQLite
uses WAL and FULL durability. An outbox in the same transaction supports recovery
without replaying effects. Durable checkpoints make normal reopen incremental;
explicit repair verifies the entire chain. Mirror failures are reported on stderr
and in store inspection.

```sh
uv run dowser journal-repair \
  --database .local/campaign/history.sqlite3 \
  --output .local/campaign/audit.jsonl
```

The same command exports a database whose mirror was previously disabled. Torn
final lines are recovered; complete corrupt records fail integrity verification.

Decisions input is $0.10 per million tokens; output and cache-specific charges are
absent on this endpoint. Long-context and regional premiums apply according to
the [official guide](https://developers.openai.com/api/docs/guides/decisions) and
[model pricing](https://developers.openai.com/api/docs/models/gpt-6-luna).
Accounting reserves each stage/retry's estimated input and reconciles reported
usage. Inputs above 272,000 tokens cost twice the base input rate for the full
request; configured regional processing adds 10%. Unknown reservations remain.
Jev retains its independent 80 requests/s and 100,000 tokens/s policy and pricing.

## Offline comparison and paid runs

`python -m dowser.compare_decisions --output .local/new-comparison` renders the
same fixtures through each provider with bounded and large memory profiles,
without model calls. Optional `--pilot-history` and `--prepared` replay preserved
nomination-only stalls through the new catalogue with read-only source access.
This measures input coverage and navigation availability, not model accuracy.
The former repetition gates and view-specific navigation were harness constraints;
a larger context alone does not establish better diagnostic quality.

The benchmark CLI defaults to OpenAI; `--provider jev` keeps Jev selectable.
Use a new campaign name and preserve existing campaigns. After separately
authorizing a paid pilot, supply the actual account limits:

```sh
uv run --extra openai --extra itbench-aa dowser-bench run \
  --provider openai --openai-rpm ACCOUNT_RPM --openai-tpm ACCOUNT_TPM \
  --phase pilot --root .local/itbench-aa-prepared-v2 --campaign NEW_CAMPAIGN
```

The 30-minute incident watchdog and bounded attempts/retries remain. A native
model refusal ends that case with score zero; the pilot retains its traces and
spending and advances to untouched cases without replay. Malformed responses and
other permanent technical failures still halt the batch.

API access,
actual accuracy, latency and billed cost require separately authorized paid
validation; offline mock results do not establish them.

See [offline verification results](decisions-verification.md) for the checked scenarios and test evidence.

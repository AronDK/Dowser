# Automatic semantic and graph memory

Semantic memory is opt-in. SQLite remains the durable execution, evidence, audit
and accounting store. A disposable Qdrant index runs in process memory; it selects
saved evidence automatically before each native `gpt-6-luna` Decisions request.
This is Dowser-native local graph retrieval, not Microsoft's full GraphRAG package
or Mem0. There is no additional generative model.

Install the relevant extras:

```sh
uv sync --locked --extra openai --extra itbench-aa --extra semantic-memory
```

In an existing configuration, replace the event-store reference and select the
semantic context profile. Supply your actual embedding limits; the numbers below
are placeholders to replace, not recommended account caps:

```json
{
  "event_store": {
    "factory": "dowser.semantic_store:semantic_store",
    "settings": {
      "path": ".local/history.sqlite3",
      "journal": ".local/audit.jsonl",
      "mode": "graph",
      "embedding_rpm": 1,
      "embedding_tpm": 1024
    }
  },
  "context_builder": {
    "factory": "dowser.core:context_builder",
    "settings": {"profile": "semantic"}
  }
}
```

These are two entries in the existing eight-slot configuration, not a complete
configuration. Add `plugins.semantic_memory:tool_plugin` to the registry instead
of `plugins.history:tool_plugin` to enable dynamic semantic recall plus filtered
history and exact artifact/page access. The Decisions provider retains its
separately configured RPM/TPM. Configure `ledger` for standalone paid embedding
accounting to choose its path; otherwise a sibling embeddings ledger is created
automatically. The benchmark supplies its campaign ledger automatically.

`OPENAI_API_KEY` or the ignored `.env` supplies the credential. Optional SDKs are
loaded only when needed. Configuration validation makes no API calls.

## Evidence and queries

Context construction synchronizes an event-sequence watermark with committed
facts, execution outcomes and hypotheses. Execution starts and raw outputs retain
their existing FULL-durability transaction boundaries. No embeddings run inside
those transactions. Retrieval never opens the platform's full snapshot index.

The renderer preserves meaningful identifiers, arguments, measurements and errors,
and removes repetitive administrative fields. It generates separate documents
for facts, actions and explicitly labeled interpretations. Records are split
structurally before splitting long text; segments retain provenance and explicit
incompleteness. Original records remain available through their event/artifact
references. Saved recall results are not indexed as new evidence.

Before each decision, queries derive from the alert and desired state, focus and
unresolved questions, recent outputs, and relevant action descriptions/arguments.
The default is eight queries, 1,024 estimated tokens per evidence chunk,
`text-embedding-3-small` with 1,536 dimensions, 20 matches per query/method, and
24 selected documents within a 16,384 estimated-token memory budget. Estimates
are explicitly separate from API-reported usage; tokenizer assets are never
downloaded during context construction.

Queries combine cosine vector matches and exact lexical matches using reciprocal
rank fusion with constant 60. Graph mode expands one hop through explicitly
admitted relationship records, attaching edge provenance. It cannot infer causal
edges or fetch uninspected neighboring evidence. Results diversify across entities,
evidence kinds and categories. Retrieval scores never grant permissions or remove
available actions.

Platform plugins may implement:

```python
async def memory_query_hints(self, state, candidates):
    # Return MemoryQueryIntent objects derived from this platform and incident.
    # Optional filters: entity, tool, evidence_kind.
    ...
```

Hints describe text and filters, not numeric vectors or a fixed symptom-to-command
dictionary. Core queries work without hints. `memory.search` choices contain a
current intent ID with strict arguments. The SOM selects the ID; the harness
generates its embedding and searches admitted evidence. Selecting recall prioritizes
that intent in the following automatic context. Exact artifact and page expansion
remain available.

## Freshness, failure and recovery

All semantic retrieval is incident-local, even when two incidents share a trusted
memory scope. ITBench trials therefore cannot retrieve another trial's evidence.
Facts retain original timestamps, snapshot fingerprints, status and references.
Different values for the same fact key/resource remain marked as contradictions.
Snapshot evidence is pinned only when the fingerprint matches the current state;
live saved evidence still needs revalidation. Interpretations remain separate.

The durable cache keys vectors by renderer version, model, dimensions and rendered
text hash. Reopen rebuilds the RAM index using this cache. Source execution is never
replayed. Unknown embedding jobs remain pending and are not automatically reissued
on restart. A received, validated response can repair an interrupted cache write
and reconcile known usage without another API call. Transient exhaustion yields
explicitly degraded coverage and lexical
retrieval over admitted evidence. Permanent API/configuration and invalid-vector
failures terminate the trial. Required state overflow remains a context failure.

Each SDK attempt disables internal retries and gets its own ledger reservation.
The cache records pending work before the remote request. API requests and received
responses enter the audit, while credentials do not. Reported embedding usage
reconciles reservations at its own per-token price; unknown reservations remain.
Retries honor rate admission, attempt/incident deadlines and Retry-After. Optional
combined account RPM/TPM can be configured on both endpoints when the account shares
a pool; endpoint limits still apply. No spending or call-count ceiling is added.

The input retains current required observations, exact action history, cumulative
progress and the complete catalogue independently of ranking. Retrieval metadata
publishes queries, coverage, selected references, omissions and expansion access.
Provider-driven trimming retains omission references. A cached result avoids
re-embedding or re-ranking an unchanged evidence/query state.

## ITBench-AA evaluation

Use fresh campaign IDs, native Decisions, and explicit pilot/full phases. No paid
evaluation is authorized merely by installing or configuring semantic memory.

| Arm | Memory |
| --- | --- |
| A | Existing large cumulative context and history tools |
| B | Deterministic bounded recent memory and dynamic recall |
| C | Vector plus lexical retrieval and dynamic recall |
| D | C plus admitted one-hop graph expansion |

B–D use identical memory budgets and recall menus. Disable assessments in the
initial comparison. Keep all non-memory settings, source revision, scoring, code,
lock file and Python version fixed. Use the same embedding settings for B–D,
although B performs no embedding calls. Use identical combined account constraints
for A too, if configured.

When the user explicitly chooses no account limits, pass
`--unlimited-account-limits` instead of RPM/TPM arguments. This disables client-side
admission for both OpenAI endpoints without inventing substitute caps. API response
handling, retries, model input limits and the incident watchdog remain active;
the setting is frozen in the campaign manifest.

After separate pilot authorization, the command for an individual arm is:

```sh
uv run --no-sync dowser-bench run \
  --provider openai --memory-arm D --phase pilot \
  --openai-rpm ACCOUNT_RPM --openai-tpm ACCOUNT_TPM \
  --embedding-rpm EMBEDDING_RPM --embedding-tpm EMBEDDING_TPM \
  --root .local/itbench-aa-prepared-v2 --campaign NEW_CAMPAIGN_D
```

The existing ten pilot cases validate operation. Corrections require fresh campaign
IDs. Full evaluation requires separate authorization and the existing technically
completed pilot gate; run each frozen arm on 40 scenarios with seeds 42/43/44.
Alternate arm order across scenarios operationally. Never replay completed failures
or discard unknown reservations. Reports separate embedding/Decisions tokens, calls,
cost and latency, along with retrieval coverage, degraded rounds and semantic recall.
Memory telemetry includes estimated vector bytes and process-wide peak resident
memory where supported; the latter is not attributed exclusively to a trial.

The offline suite includes a four-arm ITBench fixture through the actual loop,
mocked SDK transports and real Qdrant. It verifies submission, catalogue
reachability, grader exclusion and no-call resume. These results do not establish
live model understanding or diagnosis quality.

For labeled archived request prefixes, provide a JSON list with `request_ref` and
`relevant` event-reference grades. Labels are offline evaluation inputs and never
enter a model request:

```json
[{"request_ref": "pilot-s8:100", "relevant": {"pilot-s8:72": 2}}]
```

```sh
uv run --no-sync python -m dowser.evaluate_memory retrieval \
  --database .local/CAMPAIGN/history.sqlite3 --labels .local/retrieval-labels.json
uv run --no-sync python -m dowser.evaluate_memory compare \
  --a .local/CAMPAIGN_A --b .local/CAMPAIGN_B \
  --c .local/CAMPAIGN_C --d .local/CAMPAIGN_D
```

Prefix evaluation reads SQLite in read-only mode, rejects future/unowned evidence,
and measures recall@24, nDCG@24 and omission/provenance coverage. Its gate requires
90% mean recall and complete provenance/omission checks. Include relevant earlier
facts, hard negatives, dependencies, contradictions and long segments in labels.

Campaign comparison checks frozen settings, pairs scenario/seed results, and uses
10,000 scenario-clustered bootstrap samples for 95% confidence intervals. It reports
the 30 scenarios outside the pilot separately. Failed trial scores remain zero.
Promote D only after offline correctness gates pass, its held-out mean improvement
over A reaches five percentage points with an interval excluding zero, and its
improvement over C also excludes zero. If C meets the baseline gate and D does not
add demonstrated value, promote C and retain experimental graph mode. Incomplete
campaigns cannot yield a promotion recommendation. Cost and latency are reported
as tradeoffs; the default remains A until paid evaluation supports promotion.

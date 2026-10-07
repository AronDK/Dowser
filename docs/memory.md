# Cumulative investigation memory

SQLite stores original evidence and events, plus rebuildable indexed projections
of findings and execution outcomes. Schema version 2 migrates version 1
transactionally. Migration never invokes tools or a model, and leaves existing
events unchanged.

The default context builder depends on `event_store`. `DecisionRequest.memory`
contains bounded findings, their provenance, recent action outcomes and cumulative
progress counts. `recent_outcomes` controls optional full execution records, not
this memory. Trimming preserves progress, the first two findings and the latest
action; if those cannot fit, the loop reports context failure. Full evidence
remains in history and artifacts. Excerpts are display views, not replacements.

Plugins may implement optional async hooks:

- `action_identity(candidate, state) -> ActionIdentity`: a semantic key, evidence
  version and decision-state fingerprint. Exclude transient revisions only when
  they do not alter the operation. Include meaningful targets and parameters.
- `extract_memory(candidate, parsed) -> list[MemoryFact]`: structured findings
  with stable keys and resource IDs. The loop assigns admitted evidence references.

Plugins without hooks retain exact-argument repetition checks and conservative
per-observation findings. SQLite's optional `memory`, `action_count` and
`cached_reads` capabilities do not change required extension methods. Legacy
stores supply outcome summaries through their existing `history` method.

Cross-alert retrieval requires `IncidentState.memory_scope`, with an explicit
`namespace` and `partition` chosen by a trusted normalizer. Equal scope and equal
resource identities/platform versions are required. Without a scope, memory is
incident-only. Historical findings are marked `revalidation_required`; they do
not authorize changes or imply current health. Conflicting values retain both
provenances. ITBench scopes use unique trial IDs, preventing trial leakage.

ITBench read identities exclude investigation revision and distinguish external
reads from recall. Successful evidence pages are cached; recall restores an exact
stored page without rereading the snapshot. Repetition checks account for current
view and learned pages, allowing revisits after learning new evidence. Repeating
the same action in an unchanged state is blocked. Failed or unknown executions
are never replayed from cache. Incident redelivery still does not automatically
resume execution; unknown outcomes require reconciliation.

## Validation

Offline behavioral tests are in `tests/test_memory.py`. The opt-in live command
uses actual Jev choices against controlled tool evidence:

```sh
uv run --extra jev --extra itbench-aa python -m dowser.validate_memory \
  --campaign memory-validation \
  --previous-campaign .local/itbench-aa/previous-campaign --pilot
```

It tests navigation loss, prior failed probes and restored related-alert memory.
Paired conditions keep current state and candidates identical, changing only
cumulative memory. Each condition requires at least four correct choices in five
runs. Memory-omitted controls measure the effect without requiring their failure.
Requests, actual choices, failures, usage and cost persist under `focused/`.
Only a passing focused validation starts the ten-case pilot. Existing focused
results never replay automatically.

New campaigns have no monetary or call-count ceiling. Accounting retains known
usage, unknown reservations and inherited spending. Existing ledger policies
remain unchanged. The hard caps stay 80 requests/s and 100,000 tokens/s; a
30-minute incident watchdog reports unfinished execution without forcing a
diagnosis. The selected model controls nomination, submission and escalation.

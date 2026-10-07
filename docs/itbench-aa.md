# ITBench-AA through Dowser and Jev

This is **ITBench-AA public subset — Dowser/Jev adapted evaluation**, an offline
candidate-selection evaluation. It is not an official Artificial Analysis score.
The [public release](https://huggingface.co/datasets/ArtificialAnalysis/ITBench-AA)
contains 40 CC-BY-4.0 snapshots. The
[official methodology](https://artificialanalysis.ai/methodology/intelligence-benchmarking#itbench-aa)
also uses 19 private cases, shell access, and a normalization model. Cite
[IBM's original paper](https://arxiv.org/abs/2502.05352) when using the data.

## Run

```sh
uv sync --locked --extra jev --extra itbench-aa
uv run dowser-bench download
uv run dowser-bench prepare
uv run dowser-bench run --campaign my-campaign
uv run dowser-bench report .local/itbench-aa/my-campaign
```

Downloads use four workers, no Hugging Face authentication, resumable Hub
downloads, and revision `76df38a82288f75ba9e41dc8c515033332497473`. Originals live
at `~/Projects/ITBench-AA`. Preparation requires the complete remote file list,
validates all 40 ground-truth schemas, hashes every file, makes originals read-only,
and builds separate streaming SQLite indexes. It never modifies source content.
The `itbench-aa` extra supplies Hugging Face Hub and PyYAML; the `jev` extra supplies
HTTP and `.env` support. The private key stays in the existing `.env` or
`TYPESAFE_API_KEY`, as described in [Jev setup](jev.md).

The runner constructs the existing `Application`, calls the benchmark normalizer,
and runs the existing incident loop directly. It needs no alert source. Factories
are `plugins.itbench_aa:normalizer`, `:tool_plugin`, and `:decision_provider`.
Normalizers and tools share a trusted scenario/index/trial configuration; the
decision provider wraps the existing Jev adapter and advertises one decision per
round. The default context builder includes only the latest investigation
observation and zero prior execution outcomes. Complete history remains durable.

The ten pilot cases run in order: 8, 2, 19, 17, 16, 9, 7, 6, 31, 102. If technical
checks pass, all 40 cases run in numeric order three times with seeds 42, 43, 44.
Pilot correctness does not gate the full run. Unrecovered provider, parsing, submission,
context, or runtime failures stop progression. HTTP 408/429/5xx and transport failures allow two
exponential-backoff retries within the provider deadline. `--phase pilot` runs only the pilot;
`--phase full` requires an existing technically completed pilot.

## Investigation and submission

Entity pages contain twelve identities observed in snapshots. Application and
chaos namespaces precede system entities; every entity remains reachable.
Candidates contain only code-supplied, strict arguments. The model can focus,
inspect configuration/history, relationships, events, logs, traces or metrics,
page evidence, nominate or remove a factor, and submit. A nomination uses one of
six reason categories and must cite an inspected record owned by the entity.
Submission writes only the configured trial diagnosis artifact and uses the
incident's single permitted change. Investigation steps produce observations.
Current scope, action arguments, and investigation revision are revalidated before
execution. Paths reject traversal and symlinks, including ancestor symlinks.

Evidence pages are capped at 4 KiB. Large records use explicit UTF-8 segments;
configuration history also offers a fixed projection of identity, revision,
configuration and status. The single current investigation payload is capped at
8 KiB. Required state overflow terminates instead of dropping evidence. The
provider checks its complete request against a conservative byte budget.
Ground truth, `data.jsonl`, recommended actions, and grader outputs are outside
the agent index. Structured credential fields and credential-shaped log text are
redacted. The Jev adapter additionally rejects its actual key anywhere in a request.

Output uses AA's established JSON shape:

```json
{"contributing_factors":[{"name":"namespace/Kind/name","reasoning":"Code-generated explanation of the selected reason category.","evidence":"Snapshot type, timestamp and file/record references."}]}
```

The verifier checks the exact output structure, selected identities and record
ownership. **`resolved` means submission completed**, independently of whether
the selected root causes are correct. Scoring reads ground truth only after the
trial terminates, supports legacy and `spec` layouts, and merges overlapping alias
groups. Names or regex filters require namespace/kind checks. Observed ownership
links support workload equivalence; Service/Pod equivalence requires explicit
ground-truth aliases. Chaos-mesh Schedule names follow AA's documented spawned
resource rule. Ambiguous and unmatched identities
count as false positives. Equivalent predictions are deduplicated. Missing any
root-cause group scores zero; otherwise the score is `TP / (TP + FP)`.
The release's malformed scenario-38 filter `*.*` is mechanically repaired to
`.*.*` and explicitly recorded in scoring artifacts and the campaign's
methodological differences. Identical repeated group definitions are merged.

## Accounting, artifacts, and resume

Defaults are pinned `jev-1.13.0`, no call-count ceiling, a 30-minute incident
watchdog, a 15-second tool timeout, a 10-second provider timeout, one identical
semantic action per unchanged decision state, and 600-second observation freshness.
SQLite provides [cumulative memory and recall](memory.md). There is no fallback model.
HTTP 408/429/5xx and transport failures permit at most two
retries with exponential backoff, jitter and `Retry-After`/`Retry-After-ms` handling; the provider
deadline covers all attempts and delays. Invalid responses are not retried.
A transactional ledger reserves the full documented
64,000-token ceiling before each call. Known usage reconciles the reservation;
unknown usage retains it conservatively. A rejected decision with valid usage
is reconciled without admitting the decision.
New campaigns impose no monetary ceiling. Spending carried forward from previous
campaigns remains accounted. Every HTTP attempt, including a retry, gets its own
reservation; unknown billed outcomes retain it. Previously recorded ledger
policies remain unchanged.
Pricing is verified
and its source saved when creating the campaign; price changes require updating
accounting before running. Output tokens are currently free.

Artifacts live in `.local/itbench-aa/<campaign>/`: a frozen reproducibility manifest,
pricing source, SQLite history and spending ledger, diagnoses, post-trial scoring,
trial records, JSONL/CSV exports, `diagnostics.jsonl`, and `summary.md`. Failure
details persist in the ledger and incident history and are printed as JSON to
stderr, including response-validation reason codes and safe numeric details.
Pilot results remain separate.
Reports include score, exact-root-set accuracy, failure categories, calls, token
usage, accounted cost, unknown reservations, model latency, and trial duration.

Rerun the same campaign command to skip completed trials with matching dataset,
configuration, code, lock, and Python fingerprints. An interrupted trial is
recorded without automatically replaying it. A durable diagnosis can be verified
and scored without calling Jev again. Concurrent campaign writers are locked out.
Incomplete campaigns are explicitly reported with their completed trial count;
they must not be represented as a completed 120-trial evaluation.

## Offline checks

```sh
uv run python -m unittest discover -s tests -v
uvx ruff check dowser plugins tests
```

Tests use simulated Jev responses, including the full ten-pilot plus 120-trial
schedule through the actual harness, and never make paid calls.

## Rerunning after an implementation correction

Use a new campaign ID to preserve the preceding run's frozen manifest and failed
trial. Carry its entire accounted spending, including unknown reservations, into
the new cumulative cost accounting:

```sh
uv run dowser-bench run --campaign corrected-campaign \
  --previous-campaign .local/itbench-aa/previous-campaign
```

The predecessor ledger is read without modification; its hash and spending
snapshot are frozen in the new manifest. Rerunning the new campaign skips its own
completed trials, verifies the predecessor snapshot and retains all reservations.
The new report keeps predecessor spending and calls separate from the new trial
results. Chained reruns carry the total from earlier campaigns forward.

The benchmark follows the official TypeSafe SDK parser by retaining the returned
`choice` field. The API's generated schema says probability sums are approximate,
and the SDK does not enforce an arithmetic sum or recompute the chosen label.
Live responses exposed a sum of 0.99 and a selected probability of 0.35 versus a
reported maximum of 0.36. These score inconsistencies are diagnostic warnings.
Near-one sums within 0.02 are proportionally normalized for reporting; other
sums retain raw values. The native choice is never replaced with an inferred
argmax. Every warning, adjustment, raw probability and supplied choice persists
in history. Unknown labels, missing options, nonfinite/out-of-range values,
malformed fields and model mismatches still fail. The source used to verify SDK
parsing and retry behavior is revision `f078f1e208a0d885154dc758344ae4fce77ac168`.

The account's hard caps of 80 requests/s and 100,000 tokens/s are frozen in the
configuration. One shared rolling admission window covers all trials and retries.
Rate reservations use 64,000 input tokens plus 2,048 output tokens of headroom,
and reconcile reported input-plus-output usage. Rate waiting shares the provider
deadline. These limits govern this client's traffic; the API also enforces the
account limits. Financial reservations retain cumulative accounting without a monetary ceiling.

Pilot and full-run upstream HTTP 408/429/5xx, transport failures and provider deadlines
that exhaust the retry budget remain failed, scored at zero, and retain every
reservation. The batch waits 60 seconds and advances without replay. Permanent
HTTP errors, malformed responses and other technical failures still halt. Failed
pilot cases retain zero scores; full-run admission remains separately gated.

A batch-only correction can resume an existing campaign with
`--phase pilot --resume-runner-update` or `--phase full --resume-runner-update`.
The runner requires all other code hashes
and all per-trial execution/configuration code to match the archived initial
source, records a separate runner source revision in the manifest, and preserves
the initial code fingerprints and all completed trial records.

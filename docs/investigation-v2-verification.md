# Investigation improvements: offline verification

Implemented from published baseline `523f218` on 2026-10-08 in four review stages:

1. Separate aggregate decision and HTTP attempt deadlines, preserve the legacy alias,
   remove the benchmark's duplicated wrapper, and persist safe timing diagnostics.
2. Separate primary owners from associated entities, compact matching object/event
   views, collapse unchanged revisions, retain provenance and provide raw inspection.
3. Reduce state before copying, bound benchmark runtime outcomes, preserve required
   admitted observations and earlier nomination evidence, track target knowledge,
   persist hypotheses and batch non-effect diagnostics with FULL durability.
4. Add typed Choice/Noul/Score assessments and an optional benchmark consumer with
   bounded batches, durable cache keys, presentation ranking and hypothesis opinions.

The benchmark keeps SOM in control of investigation and submission. Assessments are
opt-in through `--assessments`; they cannot grant execution permissions. Generic
integrations retain full runtime state unless they opt into the reduced view.

## Checks

- **155 tracked tests and 116 private core tests passed (271 total).**
- Ruff lint and formatting checks passed.
- Source distribution and wheel built successfully.
- A real simulated HTTP response lasting more than ten seconds survived the former
  deadline. Attempt retries, aggregate/incident expiry, cancellation, Retry-After
  without an aggregate deadline and token reservations have offline regressions.
- Ownership tests exclude associated Pod records from primary Node history, retain
  event targets and decoded ConfigMap data, collapse administrative churn and keep
  full raw records retrievable.
- A 1,000-outcome synthetic history stays bounded in the live view and reconstructs
  all outcomes and observations from SQLite. Required older observations restore
  before candidate validation. Batch rollback and abrupt-process-exit tests retain
  committed execution starts and artifact ownership.
- Repetition tests distinguish repeated model decisions from duplicate external I/O.
  Relevant knowledge can re-enable actions; unrelated findings cannot.
- Typed assessment, request splitting, retries, accounting, cache reuse, contradiction
  retention and permission-boundary tests use simulated responses.

## Preparation and artifact preservation

All forty version-2 indexes and their manifest were prepared separately under
`.local/itbench-aa-prepared-v2/`. Matching sanitized source indexes were copied
read-only and ownership rebuilt; original indexes were not overwritten. Offline
inspection confirms reachable shipping `QUOTE_ADDR=quote:0000` records in case 16
and primary flagd ConfigMap records in case 7.

SHA-256 checks verified **929 preserved campaign files unchanged**, including
`.local/itbench-aa/jev-noesc-20261008/` and earlier campaigns. The original 20%
exact-accuracy baseline remains preserved. The temporary handover plan was deleted.

## Synthetic profile

The same cProfile fixture used 1,000 observations and outcomes, twenty state-copy
operations and twenty context builds. These are local synthetic measurements,
not live incident latency or accuracy results.

| Operation | Before | After |
| --- | ---: | ---: |
| Twenty state copies | 2.759 s | 0.029 s |
| Twenty context builds | 2.774 s | 0.023 s |
| 1,000 SQLite appends | 0.302 s | 0.221 s |

The context builder now chooses the reduced view before deep-copying it. Benchmark
runtime profiling also records copying, validation, context construction, dispatch,
parsing, verification and SQLite writes. Unavailable server timing stays unknown.

Logs, profiles, preparation receipts and preservation hashes are in the gitignored
`.local/investigation-v2/` directory.

## Paid validation

No paid calls, fresh pilot or live full campaign were run. Real-Jev counterfactual
validation (five repetitions per condition, at least four correct) and a fresh
pilot require separate authorization. Offline checks establish implementation
behavior; an accuracy improvement has not been measured.

## Pilot preflight follow-up

The first authorized pilot attempt stopped during all-scenario preflight before
creating any trial or paid reservation. Two expanded public-alert views exceeded
8 KiB because service identities and positive shortlist flags repeated information
already present in the alert fields. The projection now encodes each service once
through namespace/service, keeps other entity targets explicit, and retains all
alert text and source references. Informational shortlist flags remain explicit.
All forty real-snapshot initial contexts now fit (maximum alert view: 7,563 bytes).
The 17 progress tests, 15 memory tests and four assessment tests passed after the
correction. The failed preflight campaign remains preserved separately.

## Authorized pilot result

The user subsequently authorized the ten-case pilot. Campaign
`.local/itbench-aa-prepared-v2/jev-progress-pilot-20261008T144711Z/` completed all ten
cases with assessments and model-selected escalation disabled.

| Metric | Preserved baseline | New pilot |
| --- | ---: | ---: |
| Exact root-set accuracy | 2/10 | 0/10 |
| Submitted diagnoses | 7 | 7 |
| Incident runtime | 85.3 min | 48.4 min |
| HTTP attempts | 2,924 | 2,201 |
| New accounted spending | $1.637824 | $1.285751 |
| Wait decisions | 1 | 991 |
| Duplicate model contexts, excluding transient metadata | 1 | 988 |

Accuracy regressed despite shorter runtime. Cases 8, 16 and 19 exhausted navigation
and repeatedly waited until required evidence became stale. Seven other cases
submitted incorrect diagnoses. No model-selected escalation occurred. Focus
concentrated in infrastructure, with only one focus action in `otel-demo`; review
public-alert relevance and navigation identity before another paid pilot.

All 32 failed attempts were connection timeouts and retried. Their unknown
reservations remain accounted. Cumulative spending is $7.186435, including the
preserved predecessor spending. All 929 preserved campaign files passed unchanged
hash verification. The full evaluation was not started and no paid trial replayed.
Detailed metrics, per-case results and observed failure patterns are in the
campaign's `comparison.json`, `comparison.md` and `findings.json`.

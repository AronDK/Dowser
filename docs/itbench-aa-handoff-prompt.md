# Current handover — testing paused

The user ended testing on 2026-10-07. Do not resume paid tests, replay trials,
start a full campaign or run ALFworld without a new explicit instruction.

The native Jev memory validation passed 30/30 paired decisions; controls scored
9/15. Four pilot cases finished: one correct submission, two model escalations,
one provider deadline failure. Exact accuracy across these four is 25%. The fifth
case was interrupted at the user's request; the ten-case pilot is incomplete.
Sources, tests and documentation are ready for GitHub; private credentials and
runtime artifacts stay under the gitignored `.env` and `.local/` paths.

The user removed the monetary ceiling and call-count ceiling. Cumulative cost
accounting remains enabled; hard caps remain 80 requests/s and 100,000 tokens/s.
The 30-minute watchdog, bounded exponential-backoff retries, persistent error
diagnostics, scoped SQLite memory and semantic repeat checks are implemented.
See [memory](memory.md), [Jev](jev.md), [evaluation](itbench-aa.md), and the latest
[handoff](handoff.md). This current handover supersedes all instructions below.

## Archived initial handover

Historical user update on 2026-10-07:
HTTP 408/429/5xx and transport failures now permit bounded exponential-backoff retries
following TypeSafe guidance. The total ceiling is $15, including all previous
accounted spending and unknown reservations. Persistent structured diagnostics
and a benchmark rerun are explicitly authorized. Preserve previous trials and
record changed code/configuration in a new campaign manifest.

Continue the existing ITBench-AA evaluation in the main checkout at
/home/aron/Projects/Dowser. Work directly in this checkout; do not create another
worktree. Complete implementation verification and the authorized live evaluation.

Existing implementation and artifacts are in:
/home/aron/.codex/worktrees/057f/Dowser

First read these files from that worktree:
- docs/itbench-aa.md
- .local/itbench-aa/jev-1130-20261006/preparation-status.md
- .local/itbench-aa/jev-1130-20261006/preparation-status.json

The implementation is uncommitted in the source worktree. Inspect both checkouts
and merge the benchmark code, CLI, dependency changes, documentation and tests into
the main checkout while preserving existing changes in both checkouts. The main
checkout has a different HEAD, so do not overwrite existing files wholesale.
Include the runtime worker wake-up fix and its tests. Reuse the main checkout's
existing Jev adapter; reconcile differences with the source adapter as necessary.
Preserve the source worktree and do not delete its artifacts.

Current state:
- 3,351 of 3,799 pinned dataset files downloaded under ~/Projects/ITBench-AA.
- 33 complete scenario indexes in the source worktree's .local/itbench-aa/indexes/.
- Missing snapshots: 7, 8, 9, 80, 81, 83, 91.
- All 40 ground-truth schemas were checked; all 33 available initial contexts fit.
- The complete 38-test suite, including the simulated 10-pilot plus 120-trial
  campaign, passed. A subsequently added budget-failure test passed separately.
- No live pilot or full-run trial has started. Paid Jev usage is $0.

The prior session was blocked by its execution sandbox's network and filesystem
restrictions. Verify actual network connectivity and write access to the main
checkout, ~/Projects/ITBench-AA and the artifact destination before proceeding.
Keep credentials private. Use the existing .env credential without displaying it;
if necessary, read the source worktree's .env securely. Do not ask for renewed
approval of paid calls: the pilot and full campaign are authorized within the $20
total Jev ceiling. If execution policy blocks an operation, describe the specific
restriction; do not try to bypass it.

Reuse or copy the completed indexes and preparation artifacts into the main
checkout's .local/itbench-aa/ so they need not be rebuilt. Resume Hugging Face's
four-worker, unauthenticated downloader at revision
76df38a82288f75ba9e41dc8c515033332497473. Preserve original snapshot contents and
verify all 40 snapshots before any paid call. Merge/update the dependency lock and
run the appropriate regression tests, including the offline campaign.

Run the existing dowser-bench download, prepare, run and report commands. Use
campaign ID jev-1130-20261006. Retain the harness's existing safety boundaries:
closed candidates from observed entities, scoped revision checks, evidence
ownership validation, 4 KiB evidence pages, 8 KiB working state, and one permitted
submission write. A resolved outcome means submission completion; correctness is
scored afterward and must never guide another investigation round.

Live defaults and schedule:
- Jev only, pinned model jev-1.13.0; no fallback model or automatic inference retries.
- One trial at a time, at most 100 calls including waits, 600-second incident
  deadline, 15-second tool timeout, 10-second provider timeout.
- Pilot once, in order: 8, 2, 19, 17, 16, 9, 7, 6, 31, 102.
- When technical checks pass, all 40 scenarios in numeric order for three repeats,
  with candidate-ordering seeds 42, 43 and 44: 120 full-run trials.
- Low diagnostic scores do not block continuation. Correct technical failures
  before continuing; preserve failed or interrupted records without silent replay.
- Freeze verified pricing. Persist reservations for the full documented
  64,000-token request ceiling before every call; reconcile known usage and retain
  reservations for unknown outcomes. Stop before total accounted cost exceeds $20.

Use deterministic namespace/kind/name or regex matching, observed workload
ownership and explicit alias groups. Preserve documented normalization differences,
including the recorded scenario-38 leading-wildcard repair. Deduplicate equivalent
predictions; ambiguous/unmatched predictions are false positives. Score zero when
any root-cause group is missed, otherwise TP/(TP+FP). Include abandonments and
failures in the full-run mean, keeping pilot results separate.

Publish the local reproducibility manifest, history, diagnoses, scoring records,
spending ledger, JSONL/CSV results and Markdown summary under
.local/itbench-aa/jev-1130-20261006/. Label the result:
"ITBench-AA public subset — Dowser/Jev adapted evaluation".
Report full-run score, exact-root-set accuracy, failure categories, calls, tokens,
accounted cost, model latency and trial duration. Clearly distinguish incomplete
execution from a completed 120-trial evaluation. Continue until the authorized
evaluation is complete or a concrete external restriction prevents progress.

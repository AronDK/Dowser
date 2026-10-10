# Decisions implementation verification

Implemented native OpenAI Decisions with `gpt-6-luna`, explicit provider selection,
repeat execution, incident-isolated SQLite retrieval, cumulative context, grouped
selection and an automatic recoverable JSONL audit mirror. Setup and contracts are
in [the Decisions guide](openai-decisions.md).

Verification on 2026-10-09 used offline fixtures and mocked transports:

- The full tracked suite passed **172 tests** and the private core suite passed
  **116 tests**. Subsequent catalogue/history changes passed **54 focused
  regressions**, including the added cases. Tests using event-loop thread callbacks
  needed a timer in this restricted sandbox's suite runner so blocked self-pipe
  wakeups would not strand executor shutdown. This does not bypass network guards.
- Ruff lint, formatting, compilation and `git diff --check` passed. Source
  distribution and wheel builds passed.
- Native choices, waits, predicates and scores, SDK wire requests, refusals,
  malformed responses, unknown IDs, retries, cancellation, usage reconciliation,
  account limits and menus of 1,600 actions have mocked regressions. Every action
  enters selection; final winners and wait remain model choices.
- Lossless catalogue decoding restores all fields, IDs, arguments and JSON types.
  Identical history defaults retain lookup failures without inferring a new action.
  Fresh inspection executes twice in the loop regression; explicit recall avoids
  a fresh read. Browser actions remain offered after navigation and waits.
- Indexed retrieval distinguishes unused actions, failures, unknown outcomes,
  rejections and lookup failures. Filters, FTS5, pagination, artifact access and
  trial isolation are covered. Separate contradictions in one parse survive.
  Recent choices retain waits without copying full native score matrices.
- All 300 admitted fixture facts remain visible in the large profile, compared
  with 12 in the bounded profile. Overflow publishes counts and retrieval queries.
  Immutable snapshot evidence survives delayed deliberation; live evidence still
  expires. The synthetic 1,000-outcome history stays bounded in the runtime view
  and reconstructs completely from SQLite.
- Existing abrupt-exit and transaction rollback tests preserve execution start
  and raw-output ordering. Journal regressions cover torn writes, mirror failure,
  repair, corruption detection and export without replaying effects. The CLI
  exported and verified a 301-event fixture journal.

Read-only replay of the preserved pilot's nomination-only stalls restored global
navigation in cases 8, 16 and 19, offering approximately 5,770, 5,769 and 8,702
candidates respectively. Their 1,063, 1,063 and 1,764 observed entities all remain
focusable. No tools or models were replayed from those campaigns.

Initial offline context checks passed for **all 40 real snapshots**, including
candidate history. The largest complete catalogue has 9,755 actions and uses about
848,000 estimated tokens including grouped questions. These are tokenizer-based
estimates, not reported API usage or a guarantee about live account admission.

The comparison uses identical alert/state/candidate fixtures across providers and
memory profiles. It establishes input coverage and navigation behavior, not model
accuracy. Former repetition gates and view restrictions were harness constraints;
provider context, choice and account limits are separate constraints.

The existing preservation receipt verified **929 campaign files unchanged**.
The later pilot history was opened read-only and hash-checked during replay.
Receipts, rendered comparison inputs and test logs are in the gitignored
`.local/decisions-verification-final/` directory. Older verification artifacts and
campaigns remain preserved.

The offline verification above made no paid calls. On 2026-10-10 the user
authorized a fresh ten-case pilot at 500 RPM and 500,000 TPM. A live SDK smoke
call returned a valid native choice and usage. The pilot uses a new campaign and
retains watchdog failures and unknown reservations without replay. Live results
are separate from these offline coverage checks.

Admission fixes for that configuration passed **30 focused tests**, including
lossless description-template decoding, adaptive question packing without omitted
candidates, and pilot resume regressions. The effective request budget respects
both context and TPM limits; preflight checks the requested phase. Ruff lint,
formatting and whitespace checks passed for these changes.

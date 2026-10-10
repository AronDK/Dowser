# Working in Dowser

Dowser is a Python incident-investigation harness. Models choose from registered
actions; the harness validates, executes, persists and verifies them. Preserve
that division when changing providers, tools or benchmark behavior. Follow the
user's current instructions and authorization; do not ask again for work already
authorized in the conversation.

## Repository map

- `dowser/`: public models/contracts, configuration, incident loop, SQLite history,
  context construction, accounting, audit journal and benchmark runner.
- `plugins/`: decision providers, history tools, ITBench-AA and platform adapters.
- `tests/`: tracked offline regression tests using mocked models/transports.
- `docs/`: extension contracts, lifecycle, platforms and evaluation methodology.
- `.local/`: ignored fixtures, optional private tests, campaigns, logs and builds.
  These files may exist in a working checkout but are not installation requirements.

Read the relevant code and tests before editing. Start with `README.md` and
`docs/extensions.md` for contracts, `docs/lifecycle.md` for execution boundaries,
and `docs/openai-decisions.md` or `docs/itbench-aa.md` for provider/benchmark work.
Treat dated results in handoff documents as historical evidence.

## Development and verification

Python 3.12+ is supported; `pyproject.toml` and `uv.lock` define packaging. The
installable packages are `dowser` and `plugins`. Keep optional SDK dependencies
behind their extras and avoid importing them during base-only configuration
validation. Use `rg` for file/text searches.

Install only the extras relevant to the task. For Decisions and ITBench-AA:

```sh
uv sync --locked --extra openai --extra itbench-aa
uv run --no-sync python -m unittest discover -s tests -v
uvx ruff check dowser plugins tests
uvx ruff format --check dowser plugins tests
git diff --check
uv build --out-dir .local/dist
```

Start with focused tests for changed behavior. Run broader core/provider suites
when public contracts, persistence or provider inputs change. If `.local/tests`
exists, its optional core suite is run with:

```sh
uv run --no-sync python -m unittest discover -s .local/tests -v
```

Keep verification offline unless live execution is within the user's authorized
scope. Mock inference, device access and effects in tests. Check package contents
when packaging changes; secrets, private fixtures and runtime artifacts must not
enter distributions. Report what passed and any remaining verification limits.

## Execution and evidence invariants

- Prior use, failure or rejection does not veto an otherwise authorized action.
  Fresh execution and explicit history retrieval are separate actions.
- Retain scope, strict arguments, credentials, permission, precondition and effect
  budgets. Scores and confidence never grant authorization. Unknown external
  effects require reconciliation before further effects.
- Keep the global benchmark catalogue reachable from every view. Navigation,
  waits and repeated choices must not force investigation phases or submission.
- SQLite is authoritative. Commit execution start before effects and raw output
  before parsing, retain FULL durability, and preserve transactional audit outbox
  recovery. Journal repair must never replay tools.
- History retrieval stays incident/trial isolated, with trusted memory scopes.
  Do not expose arbitrary SQL, unrelated artifacts, grader outputs or suggested
  answers to the model.
- Saved live observations retain their original time. Only pinned snapshot
  evidence matching its fingerprint and scope is immutable. Keep uncertainty,
  contradictions, provenance and incomplete-segment markers in compact evidence.
- Assemble cumulative context from indexed projections and keep runtime state
  bounded. Preserve all available actions and publish omissions/retrieval references
  when evidence overflows the budget.

## Provider inputs and encodings

The OpenAI adapter uses native Decisions with `gpt-6-luna`; Jev remains separately
selectable. Do not add automatic provider/model fallback. Respect current SDK and
endpoint contracts and the configured account RPM/TPM across stages and retries.
Never treat byte counts or tokenizer estimates as exact reported usage.

Catalogue compression must retain every action ID, tool, argument, field, JSON
type and candidate order. `dowser/catalogue.py` owns encoding/decoding. Version new
formats, keep old saved inputs decodable, and explain the actual wire format in
the provider's question instructions. Test mixed tools, missing fields versus
explicit null, shared defaults, string/description references and large menus.
Keep full candidate definitions and actual rendered inputs in the audit. Native
question labels retain supplied action IDs; group probabilities are local.

## Campaigns and credentials

Store credentials in environment variables or ignored `.env` files with restrictive
permissions. Never print keys or include them in prompts, traces, commits or
examples. Use `.local/` for campaign artifacts and preserve earlier campaigns.

Paid pilots and full evaluations require user authorization for their scope. Reuse
authorization and account limits already supplied in the session. Always choose
the phase explicitly; authorizing a pilot does not authorize a full campaign.
Do not introduce monetary/model-call ceilings or change account caps without a
user instruction.

Keep an in-flight campaign on its recorded implementation and settings. Develop
input, model, execution or scoring changes for a fresh campaign. An audited
batch/report-only resume must leave per-trial execution unchanged and reuse
completed trial records, including failures and unknown billing reservations.
Never replay completed paid cases to conceal failures. Use archived sources and
captured inputs when analyzing historical requests.

Separate local preparation/admission delays, watchdog failures, HTTP failures,
native refusals and completed diagnostic quality in reports. Offline lossless
round trips and mock scores do not establish model understanding or accuracy.
Hypotheses, assessments and generated diagnosis text are interpretations, not
access to private model reasoning.

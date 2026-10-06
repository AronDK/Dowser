# Dowser

An extensible Python harness for incident investigation, validated actions, and
verification, with SQLite history. Read the [architecture blog](https://akeness.dev/).

The core Python package lives in `dowser/`. The `plugins/` package includes NX-OS diagnostics, restricted interface fixes, and
vLLM decision-provider and managed-service adapters. See the [platform guide](docs/platforms.md).

## Install

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/AronDK/FireFighter.git Dowser
cd Dowser
uv sync --locked
```

## Use

Create `.local/config.json` with all eight subsystem factories, including your own
decision provider and tool plugins; see the [extension and configuration guide](docs/extensions.md).
Put the incident JSON in `.local/incident.json` using the [incident schema](docs/extensions.md#public-schemas-and-services).

```sh
uv run dowser validate --config .local/config.json
uv run dowser run --config .local/config.json --incident .local/incident.json
uv run dowser inspect --config .local/config.json --incident-id INCIDENT_ID
uv run dowser serve --config .local/config.json
```

`validate` checks configuration without invoking models or tools. `run` emits a
JSON outcome: `resolved`, `escalated`, or `recovery_unverified`. `inspect` retrieves
stored evidence and identifies interrupted executions as unknown.

`serve` adds pluggable incident intake, async enrichment, a bounded pending queue,
and serial scheduling. Configure an `incident_source`; normalization and scheduling
default to the bundled compatibility normalizer and FIFO scheduler. A finite JSONL
source is included. Terminal outcomes appear as JSON lines on stdout, with sanitized
diagnostics on stderr. See the [intake guide](docs/intake.md) for configuration,
severity ranking, and durable completion checkpoints.

Providers can return multiple actionable decisions per response, constrained by
their selected model's advertised capability. There is no harness decision-round
count limit; incident deadlines and action limits still apply. See the
[provider contract](docs/extensions.md#context-and-provider-adapters).

Actions are read-only by default. Changes require `allow_changes: true` in policy
settings and a positive `limits.changes`. Keep local configuration and runtime data
under the gitignored `.local/` directory; SQLite defaults to `.local/history.sqlite3`.

Core alpha: platform adapters require deployment inventory and a validated SOM
profile. There is no automatic incident resumption. See [lifecycle behavior](docs/lifecycle.md) and the [verification handoff](docs/handoff.md).

# Dowser

An extensible Python harness for incident investigation, validated actions, and
verification, with SQLite history. Read the [architecture blog](https://akeness.dev/).

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
```

`validate` checks configuration without invoking models or tools. `run` emits a
JSON outcome: `resolved`, `escalated`, or `recovery_unverified`. `inspect` retrieves
stored evidence and identifies interrupted executions as unknown.

Actions are read-only by default. Changes require `allow_changes: true` in policy
settings and a positive `limits.changes`. Keep local configuration and runtime data
under the gitignored `.local/` directory; SQLite defaults to `.local/history.sqlite3`.

Core alpha: no bundled model adapter or device plugin, and no automatic incident
resumption. See [lifecycle behavior](docs/lifecycle.md) and the [verification handoff](docs/handoff.md).

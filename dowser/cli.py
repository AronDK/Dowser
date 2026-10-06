"""JSON CLI; configuration validation never constructs components."""

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path

from pydantic import ValidationError

from .config import (
    EXECUTION_SLOTS,
    INTAKE_SLOTS,
    Application,
    ConfigurationError,
    read_config,
    validate_config,
)
from .intake import IntakeRunner
from .models import normalize_incident
from .store import reject_credentials


async def dispatch(args):
    config = read_config(args.config)
    _, order = validate_config(config)
    if args.command == "validate":
        return {"valid": True, "interface_version": "1", "initialization_order": order}
    if args.command == "serve" and config.incident_source is None:
        raise ConfigurationError("serve requires an incident_source factory")
    state = None
    if args.command == "run":
        data = json.loads(args.incident.read_text())
        reject_credentials(data)
        state = normalize_incident(data)
    # Relative paths in subsystem settings resolve from the working directory.
    requested = {
        "inspect": ("event_store",),
        "run": EXECUTION_SLOTS,
        "serve": EXECUTION_SLOTS + INTAKE_SLOTS,
    }[args.command]
    async with Application(config, Path.cwd(), requested=requested) as app:
        if args.command == "serve":
            return await IntakeRunner(
                app.services,
                config.intake,
                on_result=lambda result: print(result.model_dump_json(), flush=True),
                on_diagnostic=lambda message: print(
                    json.dumps(message), file=sys.stderr, flush=True
                ),
            ).run()
        if state is not None:
            return (await app.services["incident_loop"].run(state)).model_dump(
                mode="json"
            )
        return await app.services["event_store"].inspect(args.incident_id)


async def cancellable_dispatch(args):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    installed = False
    try:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        installed = True
    except (NotImplementedError, RuntimeError):
        pass
    try:
        return await dispatch(args)
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGTERM)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dowser")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "run", "inspect", "serve"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--config", type=Path, required=True)
        if command == "run":
            sub.add_argument("--incident", type=Path, required=True)
        if command == "inspect":
            sub.add_argument("--incident-id", required=True)
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(cancellable_dispatch(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print(
            json.dumps({"error": "interrupted", "error_type": "Interrupted"}),
            file=sys.stderr,
        )
        return 130
    except BaseException as exc:
        # Error details live in typed durable events; adapters can leak secrets in strings.
        error = {"error": "operation failed", "error_type": type(exc).__name__}
        if isinstance(exc, ConfigurationError):
            error["error"] = str(exc)
        elif isinstance(exc, ValidationError):
            error["validation_errors"] = [
                {"location": list(item["loc"]), "type": item["type"]}
                for item in exc.errors(
                    include_input=False, include_context=False, include_url=False
                )
            ]
        print(json.dumps(error), file=sys.stderr)
        return 1
    if args.command == "serve":
        return 0 if result else 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

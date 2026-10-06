"""Bounded intake, queued scheduling, and durable completion checkpoints."""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field, JsonValue

from .config import IntakeSettings
from .contracts import Component, factory
from .core import EmptySettings
from .models import (
    Boundary,
    IncidentState,
    PendingIncident,
    RawIncident,
    TerminalResult,
    alert_value,
    mapped_value,
    new_id,
    normalize_incident,
)
from .store import reject_credentials


class JSONLSettings(Boundary):
    path: str
    source_id: str = Field(default="jsonl", min_length=1)


class JSONLSource(Component):
    def __init__(self, path: Path, source_id: str):
        self.path = path
        self.source_id = source_id
        self.stream = None
        self.iterator = None

    async def open(self):
        if self.stream is not None:
            raise RuntimeError("source already opened")
        self.stream = self.path.open(encoding="utf-8")

        async def records():
            for line_number, line in enumerate(self.stream, 1):
                yield RawIncident(
                    source_id=self.source_id,
                    event_id=str(line_number),
                    payload=json.loads(line),
                )

        self.iterator = records()
        return self.iterator

    async def checkpoint(self, record, result):
        pass

    async def aclose(self):
        try:
            if self.iterator is not None:
                await self.iterator.aclose()
        finally:
            if self.stream is not None:
                self.stream.close()


@factory(
    subsystem="incident_source",
    component_type=JSONLSource,
    settings_model=JSONLSettings,
)
def jsonl_source(settings, context):
    return JSONLSource(context.base_dir / settings.path, settings.source_id)


class NormalizerSettings(Boundary):
    severity_map: dict[str, JsonValue] = Field(default_factory=dict)
    priority_map: dict[str, JsonValue] = Field(default_factory=dict)


class CompatibilityNormalizer(Component):
    def __init__(self, settings: NormalizerSettings):
        self.settings = settings

    async def normalize(self, record):
        if not isinstance(record.payload, dict):
            raise ValueError("incident payload must be an object")
        data = dict(record.payload)
        incident_id = data.get("incident_id", record.event_id)
        if not isinstance(incident_id, str) or not incident_id:
            raise ValueError("incident ID must be a nonempty string")
        # Preserve already namespaced IDs; qualify legacy IDs for intake only.
        data["incident_id"] = (
            incident_id if ":" in incident_id else f"{record.source_id}:{incident_id}"
        )
        state = normalize_incident(data)
        for field in ("severity", "priority"):
            canonical = f"canonical_{field}"
            if canonical not in state.alert:
                mapping = getattr(self.settings, f"{field}_map")
                value = mapped_value(mapping, state.alert.get(field))
                if value is not None:
                    state.alert[canonical] = value
        return state


@factory(
    subsystem="normalizer",
    component_type=CompatibilityNormalizer,
    settings_model=NormalizerSettings,
)
def compatibility_normalizer(settings, context):
    return CompatibilityNormalizer(settings)


class FIFOScheduler(Component):
    async def select(self, pending):
        return min(pending, key=lambda item: item.arrival_sequence).queue_id


@factory(
    subsystem="scheduler", component_type=FIFOScheduler, settings_model=EmptySettings
)
def fifo_scheduler(settings, context):
    return FIFOScheduler()


class RankSettings(Boundary):
    severity_ranks: dict[str, int] = Field(default_factory=dict)
    priority_ranks: dict[str, int] = Field(default_factory=dict)


class MappedRankScheduler(Component):
    def __init__(self, settings: RankSettings):
        self.settings = settings

    async def select(self, pending):
        def rank(item):
            return (
                mapped_value(
                    self.settings.severity_ranks,
                    alert_value(item.state.alert, "severity"),
                    0,
                ),
                mapped_value(
                    self.settings.priority_ranks,
                    alert_value(item.state.alert, "priority"),
                    0,
                ),
                -item.arrival_sequence,
            )

        return max(pending, key=rank).queue_id


@factory(
    subsystem="scheduler",
    component_type=MappedRankScheduler,
    settings_model=RankSettings,
)
def mapped_rank_scheduler(settings, context):
    return MappedRankScheduler(settings)


def validate_normalized(value) -> IncidentState:
    if not isinstance(value, IncidentState):
        raise ValueError("normalizer must return an IncidentState or None")
    # Rebuild from JSON: Pydantic instances and mutable nested fields can bypass
    # validation, including model_construct() and in-place list/dict changes.
    state = IncidentState.model_validate(value.model_dump(mode="json", warnings=False))
    if state.phase != "observe" or state.attempts:
        raise ValueError("new incidents must start in observe without prior attempts")
    namespace, separator, identifier = state.incident_id.partition(":")
    if not separator or not namespace.strip() or not identifier.strip():
        raise ValueError("intake requires a globally namespaced incident ID")
    reject_credentials(state.model_dump(mode="json"))
    return state


class IntakeError(RuntimeError):
    """Sanitized fatal intake error; adapter exception text stays private."""


@dataclass
class _Pending:
    snapshot: PendingIncident
    record: RawIncident


class IntakeRunner:
    def __init__(
        self,
        services: dict,
        settings: IntakeSettings,
        *,
        on_result: Callable[[TerminalResult], None],
        on_diagnostic: Callable[[dict], None],
    ):
        self.source = services["incident_source"]
        self.normalizer = services["normalizer"]
        self.scheduler = services["scheduler"]
        self.incident_loop = services["incident_loop"]
        self.store = services["event_store"]
        self.settings = settings
        self.on_result = on_result
        self.on_diagnostic = on_diagnostic
        self.pending: dict[str, _Pending] = {}
        self.capacity = asyncio.Semaphore(settings.pending_capacity)
        self.changed = asyncio.Event()
        self.exhausted = False
        self.failed = False

    def diagnostic(
        self,
        kind: str,
        error: BaseException | None = None,
        *,
        incident_id: str | None = None,
    ):
        message = {"kind": kind}
        if error is not None:
            message["error_type"] = type(error).__name__
        if incident_id is not None:
            message["incident_id"] = incident_id
        self.on_diagnostic(message)

    async def _produce(self):
        try:
            iterator = await self.source.call("open", seconds=15)
            if not hasattr(iterator, "__anext__"):
                raise TypeError("source open must return an async iterator")
            sequence = 0
            while True:
                await self.capacity.acquire()
                queued = False
                try:
                    try:
                        value = await self.source.worker.call(
                            anext, iterator, seconds=None
                        )
                    except StopAsyncIteration:
                        self.exhausted = True
                        self.changed.set()
                        return
                    sequence += 1
                    try:
                        record = RawIncident.model_validate(value)
                        normalized = await self.normalizer.call(
                            "normalize",
                            record.model_copy(deep=True),
                            seconds=self.settings.normalization_seconds,
                        )
                        if normalized is None:
                            self.diagnostic("record_skipped")
                            continue
                        state = validate_normalized(normalized)
                    except TimeoutError as exc:
                        self.diagnostic("normalization_timeout", exc)
                        raise IntakeError("normalization deadline exceeded") from exc
                    except Exception as exc:
                        self.failed = True
                        self.diagnostic("normalization_failed", exc)
                        continue
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        self.diagnostic("normalization_failed", exc)
                        raise IntakeError("normalization failed") from exc
                    snapshot = PendingIncident(
                        queue_id=new_id(), arrival_sequence=sequence, state=state
                    )
                    self.pending[snapshot.queue_id] = _Pending(snapshot, record)
                    queued = True
                    self.changed.set()
                finally:
                    if not queued:
                        self.capacity.release()
        except (asyncio.CancelledError, IntakeError):
            raise
        except BaseException as exc:
            self.diagnostic("source_failed", exc)
            raise IntakeError("source stream failed") from exc

    async def _recorded(self, incident_id):
        try:
            history = await self.store.history(incident_id)
        except KeyError:
            return False, None
        for event in reversed(history):
            if event.kind == "terminated":
                result = TerminalResult.model_validate(event.payload)
                if result.incident_id != incident_id:
                    raise ValueError(
                        "stored terminal result has a different incident ID"
                    )
                return True, result
        return bool(history), None

    async def _checkpoint(self, record, result):
        for attempt in range(1, self.settings.checkpoint_attempts + 1):
            error = None
            try:
                await self.source.call(
                    "checkpoint",
                    record,
                    result.model_copy(deep=True),
                    seconds=self.settings.checkpoint_seconds,
                )
            except asyncio.CancelledError:
                await self.store.append(
                    result.incident_id,
                    "checkpoint_attempt",
                    {"attempt": attempt, "status": "interrupted"},
                )
                raise
            except BaseException as exc:
                error = exc
            payload = {
                "attempt": attempt,
                "status": "failed" if error is not None else "succeeded",
            }
            if error is not None:
                payload["error_type"] = type(error).__name__
            await self.store.append(result.incident_id, "checkpoint_attempt", payload)
            if error is None:
                return
            self.diagnostic(
                "checkpoint_attempt_failed", error, incident_id=result.incident_id
            )
            if attempt < self.settings.checkpoint_attempts:
                delays = self.settings.checkpoint_retry_delays
                delay = delays[min(attempt - 1, len(delays) - 1)] if delays else 0
                await asyncio.sleep(delay)
        self.failed = True
        self.diagnostic("checkpoint_failed", incident_id=result.incident_id)

    async def _process(self, item):
        incident_id = item.snapshot.state.incident_id
        exists, recorded = await self._recorded(incident_id)
        if exists:
            if recorded is None:
                self.failed = True
                self.diagnostic(
                    "incident_requires_reconciliation", incident_id=incident_id
                )
                return
            await self._checkpoint(item.record, recorded)
            self.on_result(recorded)
            return
        try:
            returned = TerminalResult.model_validate(
                await self.incident_loop.run(item.snapshot.state.model_copy(deep=True))
            )
            _, recorded = await self._recorded(incident_id)
            if recorded is None or returned != recorded:
                raise IntakeError(
                    "incident loop did not return its durable terminal result"
                )
        except BaseException:
            # The loop records interrupted/failed executions before re-raising.
            # Checkpoint the durable result even on that exceptional path.
            _, recorded = await self._recorded(incident_id)
            if recorded is not None:
                await self._checkpoint(item.record, recorded)
                self.on_result(recorded)
            raise
        await self._checkpoint(item.record, recorded)
        self.on_result(recorded)

    async def _consume(self):
        while True:
            if not self.pending:
                if self.exhausted:
                    return
                self.changed.clear()
                await self.changed.wait()
                continue
            snapshot = tuple(
                item.snapshot.model_copy(deep=True) for item in self.pending.values()
            )
            supplied = {item.queue_id for item in snapshot}
            try:
                selected = await self.scheduler.call(
                    "select", snapshot, seconds=self.settings.scheduler_seconds
                )
                if not isinstance(selected, str) or selected not in supplied:
                    raise ValueError("scheduler selected an unknown queue ID")
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self.diagnostic("scheduler_failed", exc)
                raise IntakeError("scheduler failed") from exc
            item = self.pending.pop(selected)
            self.capacity.release()
            await self._process(item)

    async def run(self) -> bool:
        """Return success after a finite source drains; cancel siblings on failure."""
        tasks = [
            asyncio.create_task(self._produce()),
            asyncio.create_task(self._consume()),
        ]
        try:
            await asyncio.gather(*tasks)
            return not self.failed
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

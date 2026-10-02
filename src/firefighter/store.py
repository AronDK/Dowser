"""Transactional append-only SQLite history and artifact references."""

import json
import sqlite3
import threading
from pathlib import Path

from .contracts import AppContext, Component, factory
from .models import Boundary, Event, IncidentState, now


class ExistingIncidentError(ValueError):
    pass


class StoreSettings(Boundary):
    path: str = ".local/history.sqlite3"


def reject_credentials(value):
    """Fail closed on credential-shaped structured fields; never log the value."""
    forbidden = {
        "password",
        "passwd",
        "secret",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "authorization",
        "credentials",
        "private_key",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower().replace("-", "_") in forbidden:
                raise ValueError(
                    "credential fields must stay outside persisted incident data"
                )
            reject_credentials(child)
    elif isinstance(value, list):
        for child in value:
            reject_credentials(child)


class SQLiteStore(Component):
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.connection = sqlite3.connect(path, check_same_thread=False)
        try:
            self._initialize()
        except BaseException:
            self.connection.close()
            raise

    def _initialize(self):
        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            raise ValueError("unsupported SQLite store schema version")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
                sequence INTEGER NOT NULL, schema_version TEXT NOT NULL,
                timestamp TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY (incident_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                ref TEXT PRIMARY KEY, incident_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                payload TEXT NOT NULL,
                FOREIGN KEY (incident_id, sequence) REFERENCES events(incident_id, sequence)
            );
            CREATE TRIGGER IF NOT EXISTS immutable_events_update BEFORE UPDATE ON events
            BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_events_delete BEFORE DELETE ON events
            BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_artifacts_update BEFORE UPDATE ON artifacts
            BEGIN SELECT RAISE(ABORT, 'artifacts are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_artifacts_delete BEFORE DELETE ON artifacts
            BEGIN SELECT RAISE(ABORT, 'artifacts are append-only'); END;
        """)
        self.connection.execute("PRAGMA user_version=1")

    def _append(self, incident_id, kind, payload, artifacts=None):
        reject_credentials(payload)
        reject_credentials(artifacts or {})

        def check_refs(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key == "raw_output_refs":
                        if not isinstance(child, list):
                            raise ValueError("raw_output_refs must be a list")
                        for ref in child:
                            exists = (
                                ref in (artifacts or {})
                                or self.connection.execute(
                                    "SELECT 1 FROM artifacts WHERE ref=? AND incident_id=?",
                                    (ref, incident_id),
                                ).fetchone()
                            )
                            if not exists:
                                raise ValueError(
                                    "unknown raw output artifact reference"
                                )
                    else:
                        check_refs(child)
            elif isinstance(value, list):
                for child in value:
                    check_refs(child)

        check_refs(payload)
        sequence = self.connection.execute(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM events WHERE incident_id=?",
            (incident_id,),
        ).fetchone()[0]
        event = Event(
            incident_id=incident_id,
            sequence=sequence,
            timestamp=now(),
            kind=kind,
            payload=payload,
        )
        self.connection.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            (
                incident_id,
                sequence,
                "1",
                event.timestamp.isoformat(),
                kind,
                json.dumps(event.payload),
            ),
        )
        for ref, artifact in (artifacts or {}).items():
            self.connection.execute(
                "INSERT INTO artifacts VALUES (?,?,?,?)",
                (ref, incident_id, sequence, json.dumps(artifact)),
            )
        return event

    async def ingest(self, state):
        with self.lock, self.connection:
            # Lock before reading/writing the incident to serialize other connections.
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self.connection.execute(
                    "INSERT INTO incidents VALUES (?,?)", (state.incident_id, "1")
                )
            except sqlite3.IntegrityError as exc:
                raise ExistingIncidentError(
                    "incident ID already exists; automatic resumption is disabled"
                ) from exc
            return self._append(
                state.incident_id,
                "incident_ingested",
                {"state": state.model_dump(mode="json")},
            )

    async def append(self, incident_id, kind, payload, artifacts=None):
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            return self._append(incident_id, kind, payload, artifacts)

    async def history(self, incident_id):
        with self.lock:
            rows = self.connection.execute(
                "SELECT sequence,schema_version,timestamp,kind,payload FROM events WHERE incident_id=? ORDER BY sequence",
                (incident_id,),
            ).fetchall()
        if not rows:
            raise KeyError("incident not found")
        return [
            Event(
                incident_id=incident_id,
                sequence=s,
                schema_version=v,
                timestamp=t,
                kind=k,
                payload=json.loads(p),
            )
            for s, v, t, k, p in rows
        ]

    async def reconstruct(self, incident_id):
        events = await self.history(incident_id)
        state = IncidentState.model_validate(events[0].payload["state"])
        starts = {}
        for event in events[1:]:
            if event.kind == "state_transition":
                state.phase = event.payload["phase"]
            elif event.kind == "execution_result":
                state.attempts.append(event.payload)
                starts.pop(event.payload["execution_id"], None)
            elif event.kind == "execution_started":
                starts[event.payload["execution_id"]] = {
                    k: v for k, v in event.payload.items() if k != "candidate"
                }
            elif event.kind == "parse_outcome":
                from .models import ParseResult

                parsed = ParseResult.model_validate(event.payload["parse"])
                state.observations.extend(parsed.observations)
        state.attempts.extend(
            {**start, "status": "unknown"} for start in starts.values()
        )
        return state

    async def inspect(self, incident_id):
        events = await self.history(incident_id)
        starts = {
            e.payload["execution_id"]: e.payload
            for e in events
            if e.kind == "execution_started"
        }
        results = {
            e.payload["execution_id"]: e.payload
            for e in events
            if e.kind == "execution_result"
        }
        executions = [
            {
                **start,
                "outcome": results.get(eid),
                "status": results[eid]["status"] if eid in results else "unknown",
            }
            for eid, start in starts.items()
        ]
        with self.lock:
            artifacts = {
                r: json.loads(p)
                for r, p in self.connection.execute(
                    "SELECT ref,payload FROM artifacts WHERE incident_id=? ORDER BY sequence,ref",
                    (incident_id,),
                )
            }
        return {
            "state": (await self.reconstruct(incident_id)).model_dump(mode="json"),
            "events": [e.model_dump(mode="json") for e in events],
            "executions": executions,
            "artifacts": artifacts,
        }

    async def aclose(self):
        with self.lock:
            self.connection.close()


@factory(
    subsystem="event_store", component_type=SQLiteStore, settings_model=StoreSettings
)
def sqlite_store(settings: StoreSettings, context: AppContext):
    return SQLiteStore(context.base_dir / settings.path)

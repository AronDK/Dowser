"""Transactional append-only SQLite history and artifact references."""

import json
import sqlite3
import threading
from pathlib import Path

from .contracts import AppContext, Component, factory
from .memory import action_identity, compact, facts_from_parse, scope_key
from .models import ActionCandidate, Boundary, Event, IncidentState, ParseResult, now


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
        if version not in (0, 1, 2, 3):
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
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS memory_scopes (incident_id TEXT PRIMARY KEY REFERENCES incidents(incident_id), scope TEXT NOT NULL)"
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS memory_scope_lookup ON memory_scopes(scope)"
            )
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS memory_facts (incident_id TEXT NOT NULL, sequence INTEGER NOT NULL, item INTEGER NOT NULL, fact_key TEXT NOT NULL, resource_id TEXT NOT NULL, payload TEXT NOT NULL, refs TEXT NOT NULL, status TEXT NOT NULL, PRIMARY KEY(incident_id,sequence,item), FOREIGN KEY(incident_id,sequence) REFERENCES events(incident_id,sequence))"
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS memory_fact_lookup ON memory_facts(incident_id,fact_key,sequence)"
            )
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS memory_actions (incident_id TEXT NOT NULL, sequence INTEGER NOT NULL, execution_id TEXT NOT NULL, identity TEXT NOT NULL, version TEXT NOT NULL, decision_state TEXT NOT NULL, tool TEXT NOT NULL, args TEXT NOT NULL, status TEXT NOT NULL, parse_status TEXT, PRIMARY KEY(incident_id,execution_id), FOREIGN KEY(incident_id,sequence) REFERENCES events(incident_id,sequence))"
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS memory_action_lookup ON memory_actions(incident_id,identity,version,decision_state)"
            )
            columns = {
                row[1]
                for row in self.connection.execute("PRAGMA table_info(memory_actions)")
            }
            if "detail" not in columns:
                self.connection.execute(
                    "ALTER TABLE memory_actions ADD COLUMN detail TEXT NOT NULL DEFAULT '{}'"
                )
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS memory_hypotheses(incident_id TEXT NOT NULL,entity TEXT NOT NULL,sequence INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(incident_id,entity),FOREIGN KEY(incident_id,sequence) REFERENCES events(incident_id,sequence))"
            )
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS som_assessments(incident_id TEXT NOT NULL,cache_key TEXT NOT NULL,sequence INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(incident_id,cache_key),FOREIGN KEY(incident_id,sequence) REFERENCES events(incident_id,sequence))"
            )
            if version < 2:
                for incident, sequence, kind, payload in self.connection.execute(
                    "SELECT incident_id,sequence,kind,payload FROM events ORDER BY incident_id,sequence"
                ).fetchall():
                    self._project(incident, sequence, kind, json.loads(payload))
            if version == 2:
                for incident, seq, payload in self.connection.execute(
                    "SELECT incident_id,sequence,payload FROM memory_facts ORDER BY rowid"
                ).fetchall():
                    self._hypothesis_fact(incident, seq, json.loads(payload))
            self.connection.execute("PRAGMA user_version=3")

    def _hypothesis_fact(self, incident, sequence, fact):
        entity = fact.get("entity")
        if not entity:
            return
        prior = self.connection.execute(
            "SELECT payload FROM memory_hypotheses WHERE incident_id=? AND entity=?",
            (incident, entity),
        ).fetchone()
        hypothesis = (
            json.loads(prior[0])
            if prior
            else {
                "entity": entity,
                "supporting_facts": [],
                "refuting_facts": [],
                "coverage": [],
                "outcomes": {},
                "unresolved_questions": [
                    "Is this entity causally relevant?",
                    "Is the inspected evidence sufficient?",
                    "Which observations support or refute an independent cause?",
                ],
            }
        )
        hypothesis["coverage"] = sorted(
            set(hypothesis["coverage"]) | {fact.get("kind", "unknown")}
        )
        hypothesis["evidence_digest"] = fact.get("semantic_fingerprints", [])
        hypothesis["outcomes"] = dict(
            self.connection.execute(
                "SELECT status,COUNT(*) FROM memory_actions WHERE incident_id=? AND json_extract(args,'$.entity')=? GROUP BY status",
                (incident, entity),
            )
        )
        self.connection.execute(
            "INSERT OR REPLACE INTO memory_hypotheses VALUES(?,?,?,?)",
            (incident, entity, sequence, json.dumps(hypothesis)),
        )

    def _project(self, incident, sequence, kind, payload):
        facts = []
        if kind == "som_assessment":
            self.connection.execute(
                "INSERT OR REPLACE INTO som_assessments VALUES(?,?,?,?)",
                (
                    incident,
                    payload["cache_key"],
                    sequence,
                    json.dumps(payload["result"]),
                ),
            )
        elif kind == "hypothesis_assessment":
            self.connection.execute(
                "INSERT OR REPLACE INTO memory_hypotheses VALUES(?,?,?,?)",
                (
                    incident,
                    payload["entity"],
                    sequence,
                    json.dumps(payload["hypothesis"]),
                ),
            )
        elif kind == "incident_ingested":
            state = IncidentState.model_validate(payload["state"])
            self.connection.execute(
                "INSERT INTO memory_scopes VALUES (?,?)", (incident, scope_key(state))
            )
            facts = facts_from_parse(
                ParseResult(
                    status="valid",
                    parser_version="memory/1",
                    observations=state.observations,
                )
            )
        elif kind == "execution_started":
            candidate = ActionCandidate.model_validate(payload["candidate"])
            identity = payload.get("identity", action_identity(candidate).model_dump())
            self.connection.execute(
                "INSERT INTO memory_actions(incident_id,sequence,execution_id,identity,version,decision_state,tool,args,status,parse_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    incident,
                    sequence,
                    payload["execution_id"],
                    identity["key"],
                    identity.get("evidence_version", ""),
                    identity.get("decision_state", ""),
                    candidate.tool,
                    json.dumps(candidate.args),
                    "unknown",
                    None,
                ),
            )
        elif kind == "execution_result":
            self.connection.execute(
                "UPDATE memory_actions SET status=?,parse_status=?,detail=? WHERE incident_id=? AND execution_id=?",
                (
                    payload["status"],
                    payload["parse"]["status"],
                    json.dumps(
                        {
                            "execution": payload.get("detail", ""),
                            "parse": payload["parse"].get("reason", ""),
                        }
                    ),
                    incident,
                    payload["execution_id"],
                ),
            )
        elif kind == "parse_outcome":
            facts = facts_from_parse(ParseResult.model_validate(payload["parse"]))
        for item, fact in enumerate(facts):
            self._hypothesis_fact(incident, sequence, fact.payload)
            self.connection.execute(
                "INSERT INTO memory_facts VALUES (?,?,?,?,?,?,?,?)",
                (
                    incident,
                    sequence,
                    item,
                    fact.key,
                    fact.resource_id,
                    json.dumps(fact.payload),
                    json.dumps(fact.evidence_refs),
                    fact.status,
                ),
            )

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
        self._project(incident_id, sequence, kind, payload)
        return event

    async def action_count(self, incident_id, identity):
        with self.lock:
            return self.connection.execute(
                "SELECT COUNT(*) FROM memory_actions WHERE incident_id=? AND identity=? AND version=? AND decision_state=?",
                (
                    incident_id,
                    identity.key,
                    identity.evidence_version,
                    identity.decision_state,
                ),
            ).fetchone()[0]

    async def memory(self, state, candidates, limit=12):
        """Only explicit equal scopes can retrieve another incident's evidence."""
        with self.lock:
            scope = self.connection.execute(
                "SELECT scope FROM memory_scopes WHERE incident_id=?",
                (state.incident_id,),
            ).fetchone()
            if scope and scope[0] != scope_key(state):
                raise ValueError("memory lookup changed incident scope")
            # Also supports assembling a related new alert before it is ingested.
            scope = scope[0] if scope else scope_key(state)
            incidents = [
                r[0]
                for r in self.connection.execute(
                    "SELECT incident_id FROM memory_scopes WHERE scope=?", (scope,)
                )
            ]
            if not incidents:
                return {
                    "facts": [],
                    "actions": [],
                    "progress": {"actions": 0, "facts": 0},
                    "historical": False,
                }
            marks = ",".join("?" for _ in incidents)
            rows = self.connection.execute(
                f"SELECT incident_id,sequence,fact_key,resource_id,payload,refs,status FROM memory_facts WHERE incident_id IN ({marks}) ORDER BY rowid DESC",
                incidents,
            ).fetchall()
            hypotheses_rows = self.connection.execute(
                f"SELECT incident_id,payload FROM memory_hypotheses WHERE incident_id IN ({marks}) ORDER BY sequence DESC LIMIT 8",
                incidents,
            ).fetchall()
            actions = self.connection.execute(
                f"SELECT incident_id,sequence,identity,tool,args,status,parse_status,detail FROM memory_actions WHERE incident_id IN ({marks}) ORDER BY rowid DESC",
                incidents,
            ).fetchall()
        relevant = {c.args.get("entity") for c in candidates if c.args.get("entity")}
        # Related focus first, followed by recent cumulative findings. Conflicts
        # retain both values, rather than silently treating the latest as truth.
        latest, conflicts, facts = {}, set(), []
        for incident, seq, key, resource, payload, refs, status in rows:
            value = json.loads(payload)
            fact = {
                "key": key,
                "resource_id": resource,
                "payload": compact(value),
                "evidence_refs": json.loads(refs),
                "event_ref": f"{incident}:{seq}",
                "historical": incident != state.incident_id,
                "freshness": "revalidation_required"
                if incident != state.incident_id
                else "current_incident",
                "status": status,
            }
            prior = latest.get(key)
            if prior is not None:
                if prior["value"] != value:
                    conflicts.add(key)
                    fact["status"] = "contradicted"
                    prior["fact"]["status"] = "contradicted"
                    facts.append(fact)
                continue
            latest[key] = {"value": value, "fact": fact}
            facts.append(fact)
        facts.sort(
            key=lambda f: (
                f["status"] == "contradicted",
                f["payload"].get("entity") in relevant,
            ),
            reverse=True,
        )
        selected = facts[:limit]
        recent = [
            {
                "event_ref": f"{incident}:{seq}",
                "identity": ident,
                "tool": tool,
                "args": compact(json.loads(args), 350),
                "status": status,
                "parse_status": parsed,
                "detail": compact(json.loads(detail), 350),
                "historical": incident != state.incident_id,
            }
            for incident, seq, ident, tool, args, status, parsed, detail in actions[:8]
        ]
        hypotheses = [
            {**json.loads(payload), "historical": incident != state.incident_id}
            for incident, payload in hypotheses_rows
        ]
        hypotheses.sort(key=lambda h: h["entity"] in relevant, reverse=True)
        return {
            "hypotheses": hypotheses,
            "facts": selected,
            "actions": recent,
            "progress": {
                "actions": len(actions),
                "current_incident_actions": sum(
                    a[0] == state.incident_id for a in actions
                ),
                "facts": len(latest),
                "omitted_facts": max(0, len(facts) - len(selected)),
                "contradictions": sorted(conflicts)[:12],
                "contradiction_count": len(conflicts),
                "outcomes": {
                    s: sum(a[5] == s for a in actions)
                    for s in ("succeeded", "failed", "partial", "unknown")
                },
            },
            "historical": any(f["historical"] for f in selected)
            or any(a["historical"] for a in recent),
        }

    async def observations(self, incident_id, ids):
        """Retrieve only required admitted observations in this incident."""
        if not ids:
            return []
        marks = ",".join("?" for _ in ids)
        with self.lock:
            rows = self.connection.execute(
                f"SELECT j.value FROM events e JOIN json_each(CASE WHEN e.kind='incident_ingested' THEN json_extract(e.payload,'$.state.observations') ELSE json_extract(e.payload,'$.parse.observations') END) j WHERE e.incident_id=? AND e.kind IN ('incident_ingested','parse_outcome') AND json_extract(j.value,'$.id') IN ({marks}) ORDER BY e.sequence",
                [incident_id, *ids],
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    async def assessment_cache(self, incident_id, key):
        with self.lock:
            row = self.connection.execute(
                "SELECT payload FROM som_assessments WHERE incident_id=? AND cache_key=?",
                (incident_id, key),
            ).fetchone()
        return json.loads(row[0]) if row else None

    async def cached_reads(self, incident_id):
        """Return successful read artifacts only; changes/unknowns never replay."""
        with self.lock:
            rows = self.connection.execute(
                "SELECT a.identity,r.payload FROM memory_actions a JOIN events e ON e.incident_id=a.incident_id AND e.sequence=a.sequence JOIN artifacts r ON r.incident_id=a.incident_id AND r.ref=a.incident_id || '/' || a.execution_id || '/raw' WHERE a.incident_id=? AND a.status='succeeded' AND a.parse_status='valid' AND json_extract(e.payload,'$.candidate.effect')='read_only'",
                (incident_id,),
            ).fetchall()
        return {key: json.loads(payload) for key, payload in rows}

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

    async def append_many(self, incident_id, events):
        allowed = {
            "validation_outcome",
            "candidate_snapshot",
            "provider_request",
            "provider_capabilities",
            "context_check",
            "harness_timing",
        }
        if any(kind not in allowed for kind, payload in events):
            raise ValueError("batch contains an effect or evidence admission boundary")
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            return [
                self._append(incident_id, kind, payload) for kind, payload in events
            ]

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

"""Indexed, incident-isolated retrieval. Public queries are data, never SQL."""

import json

from pydantic import ConfigDict, Field

from .memory import scope_key
from .models import Boundary


class HistoryQuery(Boundary):
    model_config = ConfigDict(strict=True, extra="forbid")
    category: str = Field(
        default="all", pattern=r"^(all|action|fact|rejection|choice)$"
    )
    text: str = ""
    entity: str = ""
    tool: str = ""
    outcome: str = ""
    evidence_kind: str = ""
    after: str = ""
    before: str = ""
    artifact_ref: str = ""
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=200)


class HistoryProjection:
    def initialize_history(self):
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS history_entries (
                id INTEGER PRIMARY KEY, incident_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                category TEXT NOT NULL, entity TEXT NOT NULL, tool TEXT NOT NULL,
                outcome TEXT NOT NULL, evidence_kind TEXT NOT NULL, timestamp TEXT NOT NULL,
                identity TEXT NOT NULL, version TEXT NOT NULL, execution_id TEXT NOT NULL,
                payload TEXT NOT NULL, UNIQUE(incident_id,sequence,category,entity,identity)
            );
            CREATE INDEX IF NOT EXISTS history_identity ON history_entries(incident_id,identity,sequence);
            CREATE INDEX IF NOT EXISTS history_filters ON history_entries(incident_id,category,entity,tool,outcome,evidence_kind,sequence);
            CREATE INDEX IF NOT EXISTS history_execution ON history_entries(incident_id,execution_id);
            CREATE INDEX IF NOT EXISTS history_chronology ON history_entries(incident_id,timestamp);
            CREATE VIRTUAL TABLE IF NOT EXISTS history_fts USING fts5(text);
            CREATE TABLE IF NOT EXISTS history_projection_version(version INTEGER);
        """)
        with self.connection:
            version = self.connection.execute(
                "SELECT version FROM history_projection_version"
            ).fetchone()
            if version and version[0] < 2:
                self.connection.execute(
                    "DELETE FROM history_fts WHERE rowid IN (SELECT id FROM history_entries WHERE category='fact')"
                )
                self.connection.execute(
                    "DELETE FROM history_entries WHERE category='fact'"
                )
                for (
                    incident,
                    sequence,
                    kind,
                    payload,
                    timestamp,
                ) in self.connection.execute(
                    "SELECT incident_id,sequence,kind,payload,timestamp FROM events WHERE kind IN ('incident_ingested','parse_outcome') ORDER BY incident_id,sequence"
                ).fetchall():
                    self.project_history(
                        incident, sequence, kind, json.loads(payload), timestamp
                    )
                self.connection.execute(
                    "UPDATE history_projection_version SET version=2"
                )
            if version and version[0] < 3:
                for (
                    incident,
                    sequence,
                    kind,
                    payload,
                    timestamp,
                ) in self.connection.execute(
                    "SELECT incident_id,sequence,kind,payload,timestamp FROM events WHERE kind IN ('provider_result','wait_started') ORDER BY incident_id,sequence"
                ).fetchall():
                    self.project_history(
                        incident, sequence, kind, json.loads(payload), timestamp
                    )
                self.connection.execute(
                    "UPDATE history_projection_version SET version=3"
                )
            if not version:
                for (
                    incident,
                    sequence,
                    kind,
                    payload,
                    timestamp,
                ) in self.connection.execute(
                    "SELECT incident_id,sequence,kind,payload,timestamp FROM events ORDER BY incident_id,sequence"
                ).fetchall():
                    self.project_history(
                        incident, sequence, kind, json.loads(payload), timestamp
                    )
                self.connection.execute(
                    "INSERT INTO history_projection_version VALUES(3)"
                )

    def project_history(self, incident, sequence, kind, payload, timestamp):
        entries = []
        if kind == "provider_result":
            result = payload["result"]
            for item, decision in enumerate(result.get("decisions", [result])):
                choice = {
                    key: decision.get(key)
                    for key in ("operation", "candidate_id", "reason", "wait_seconds")
                }
                metadata = decision.get("score_metadata", {})
                choice.update(
                    request_id=payload.get("request_id"),
                    provider=metadata.get("provider"),
                    model=metadata.get("model"),
                    interpretation=True,
                )
                entries.append(
                    (
                        "choice",
                        "",
                        "",
                        decision["operation"],
                        "",
                        f"{sequence}:{item}",
                        "",
                        "",
                        choice,
                    )
                )
        elif kind == "wait_started":
            entries.append(
                (
                    "choice",
                    "",
                    "",
                    "wait",
                    "",
                    str(sequence),
                    "",
                    "",
                    {**payload, "operation": "wait", "interpretation": True},
                )
            )
        if kind == "execution_started":
            c = payload["candidate"]
            identity = payload.get("identity", {})
            entries.append(
                (
                    "action",
                    c["args"].get("entity", ""),
                    c["tool"],
                    "unknown",
                    "",
                    identity.get("key", ""),
                    identity.get("evidence_version", ""),
                    payload["execution_id"],
                    {
                        "candidate": c,
                        "identity": identity,
                        "execution_id": payload["execution_id"],
                        "status": "unknown",
                        "started_at": timestamp,
                        "raw_output_refs": [],
                    },
                )
            )
        elif (
            kind == "validation_outcome"
            and not payload["allowed"]
            and "candidate" in payload
        ):
            c = payload["candidate"]
            identity = payload["identity"]
            entries.append(
                (
                    "rejection",
                    c["args"].get("entity", ""),
                    c["tool"],
                    "rejected",
                    "",
                    identity["key"],
                    identity.get("evidence_version", ""),
                    "",
                    payload,
                )
            )
        elif (
            kind in {"execution_result", "raw_output", "execution_unknown"}
            and "execution_id" in payload
        ):
            row = self.connection.execute(
                "SELECT id,payload FROM history_entries WHERE incident_id=? AND execution_id=? AND category='action'",
                (incident, payload["execution_id"]),
            ).fetchone()
            if row:
                value = json.loads(row[1])
                if kind == "raw_output":
                    value["raw_output_refs"] = list(
                        dict.fromkeys(
                            value["raw_output_refs"]
                            + payload.get("raw_output_refs", [])
                        )
                    )
                elif kind == "execution_result":
                    value.update(
                        outcome=payload, status=payload["status"], finished_at=timestamp
                    )
                    value["raw_output_refs"] = list(
                        dict.fromkeys(
                            value["raw_output_refs"]
                            + payload.get("raw_output_refs", [])
                        )
                    )
                else:
                    value.update(
                        status="unknown", unknown_reason=payload.get("reason", "")
                    )
                self.connection.execute(
                    "UPDATE history_entries SET outcome=?,payload=? WHERE id=?",
                    (value["status"], json.dumps(value), row[0]),
                )
                self.connection.execute(
                    "DELETE FROM history_fts WHERE rowid=?", (row[0],)
                )
                self.connection.execute(
                    "INSERT INTO history_fts(rowid,text) VALUES(?,?)",
                    (row[0], json.dumps(value)),
                )
        if kind in {"parse_outcome", "incident_ingested"}:
            observations = (
                payload.get("parse", {}).get("observations", [])
                if kind == "parse_outcome"
                else payload.get("state", {}).get("observations", [])
            )
            for item, key, resource, content, refs, status in self.connection.execute(
                "SELECT item,fact_key,resource_id,payload,refs,status FROM memory_facts WHERE incident_id=? AND sequence=?",
                (incident, sequence),
            ):
                value = json.loads(content)
                evidence_refs = json.loads(refs)
                original = next(
                    (
                        o
                        for o in observations
                        if o.get("id") in evidence_refs
                        and o.get("resource_id") == resource
                    ),
                    {},
                )
                evidence_version = original.get("immutable_snapshot") or ""
                entries.append(
                    (
                        "fact",
                        value.get("entity", resource),
                        "",
                        status,
                        value.get("kind", original.get("kind", "")),
                        f"fact:{item}:{key}",
                        evidence_version,
                        "",
                        {
                            "observed_at": original.get("observed_at"),
                            "immutable_snapshot": original.get("immutable_snapshot"),
                            "key": key,
                            "item": item,
                            "resource_id": resource,
                            "payload": value,
                            "evidence_refs": json.loads(refs),
                            "status": status,
                        },
                    )
                )
        for (
            category,
            entity,
            tool,
            outcome,
            evidence_kind,
            identity,
            version,
            execution,
            value,
        ) in entries:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO history_entries(incident_id,sequence,category,entity,tool,outcome,evidence_kind,timestamp,identity,version,execution_id,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    incident,
                    sequence,
                    category,
                    entity,
                    tool,
                    outcome,
                    evidence_kind,
                    timestamp,
                    identity,
                    version,
                    execution,
                    json.dumps(value),
                ),
            )
            if cursor.rowcount:
                self.connection.execute(
                    "INSERT INTO history_fts(rowid,text) VALUES(?,?)",
                    (cursor.lastrowid, json.dumps(value)),
                )

    def history_scope(self, state):
        row = self.connection.execute(
            "SELECT scope FROM memory_scopes WHERE incident_id=?", (state.incident_id,)
        ).fetchone()
        if row is None or row[0] != scope_key(state):
            raise ValueError(
                "history requires an ingested incident with unchanged trusted scope"
            )
        # Tool and artifact history is always limited to this incident/trial.
        return state.incident_id

    def saved_result(self, incident, refs):
        return {
            ref: json.loads(row[0])
            for ref in refs
            if (
                row := self.connection.execute(
                    "SELECT payload FROM artifacts WHERE incident_id=? AND ref=?",
                    (incident, ref),
                ).fetchone()
            )
        }

    async def action_history(self, state, identities):
        with self.lock:
            incident = self.history_scope(state)
            result = {}
            for candidate_id, identity in identities.items():
                rows = self.connection.execute(
                    "SELECT category,sequence,timestamp,version,payload FROM history_entries WHERE incident_id=? AND identity=? AND category IN ('action','rejection') ORDER BY sequence DESC LIMIT 20",
                    (incident, identity.key),
                ).fetchall()
                if not rows:
                    result[candidate_id] = {
                        "lookup_status": "ok",
                        "record_status": "no_previous_record",
                        "usage_count": 0,
                        "rejection_count": 0,
                    }
                    continue
                entries = [
                    {
                        "category": cat,
                        "event_ref": f"{incident}:{seq}",
                        "timestamp": timestamp,
                        "evidence_version": version,
                        "older_evidence_version": version != identity.evidence_version,
                        **json.loads(payload),
                    }
                    for cat, seq, timestamp, version, payload in rows[:20]
                ]
                counts = self.connection.execute(
                    "SELECT category,outcome,COUNT(*) FROM history_entries WHERE incident_id=? AND identity=? AND category IN ('action','rejection') GROUP BY category,outcome",
                    (incident, identity.key),
                ).fetchall()
                last_result = self.connection.execute(
                    "SELECT payload FROM history_entries WHERE incident_id=? AND identity=? AND category='action' AND json_array_length(json_extract(payload,'$.raw_output_refs'))>0 ORDER BY sequence DESC LIMIT 1",
                    (incident, identity.key),
                ).fetchone()
                latest = json.loads(last_result[0]) if last_result else None
                result[candidate_id] = {
                    "lookup_status": "ok",
                    "record_status": "recorded" if rows else "no_previous_record",
                    "identity": identity.model_dump(mode="json"),
                    "usage_count": sum(
                        count
                        for category, outcome, count in counts
                        if category == "action"
                    ),
                    "rejection_count": sum(
                        count
                        for category, outcome, count in counts
                        if category == "rejection"
                    ),
                    "outcomes": {
                        outcome: count
                        for category, outcome, count in counts
                        if category == "action"
                    },
                    "entries": entries,
                    "omitted_entries": max(0, sum(c[2] for c in counts) - len(entries)),
                    "latest_saved_result": self.saved_result(
                        incident, latest["raw_output_refs"]
                    )
                    if latest
                    else {},
                    "latest_saved_result_origin": {
                        "started_at": latest.get("started_at"),
                        "evidence_version": latest.get("identity", {}).get(
                            "evidence_version", ""
                        ),
                        "older_evidence_version": latest.get("identity", {}).get(
                            "evidence_version", ""
                        )
                        != identity.evidence_version,
                    }
                    if latest
                    else None,
                    "retrieval_query": {"tool": entries[0]["candidate"]["tool"]}
                    if entries
                    else {},
                }
            return result

    async def query_history(self, state, query):
        q = HistoryQuery.model_validate(query)
        with self.lock:
            incident = self.history_scope(state)
            if q.artifact_ref:
                artifact = self.saved_result(incident, [q.artifact_ref])
                if not artifact:
                    raise ValueError("artifact unavailable in current incident")
                return {"lookup_status": "ok", "artifacts": artifact}
            clauses, params = ["h.incident_id=?"], [incident]
            for key in ("category", "entity", "tool", "outcome", "evidence_kind"):
                value = getattr(q, key)
                if value and value != "all":
                    clauses.append(f"h.{key}=?")
                    params.append(value)
            for key, op in (("after", ">="), ("before", "<=")):
                if getattr(q, key):
                    clauses.append(f"h.timestamp{op}?")
                    params.append(getattr(q, key))
            if q.text:
                # Plain terms only. No caller-supplied FTS expressions or SQL.
                terms = q.text.split()
                if terms:
                    clauses.append(
                        "h.id IN (SELECT rowid FROM history_fts WHERE history_fts MATCH ?)"
                    )
                    params.append(
                        " AND ".join('"' + t.replace('"', '""') + '"' for t in terms)
                    )
            where = " AND ".join(clauses)
            total = self.connection.execute(
                f"SELECT COUNT(*) FROM history_entries h WHERE {where}", params
            ).fetchone()[0]
            rows = self.connection.execute(
                f"SELECT sequence,category,timestamp,version,payload,entity,tool,outcome,evidence_kind FROM history_entries h WHERE {where} ORDER BY sequence DESC,id DESC LIMIT ? OFFSET ?",
                [*params, q.limit, q.offset],
            ).fetchall()
            return {
                "lookup_status": "ok",
                "total": total,
                "offset": q.offset,
                "next_offset": q.offset + len(rows)
                if q.offset + len(rows) < total
                else None,
                "entries": [
                    {
                        "event_ref": f"{incident}:{seq}",
                        "category": cat,
                        "timestamp": ts,
                        "evidence_version": version,
                        "entity": entity,
                        "tool": tool,
                        "outcome_status": outcome,
                        "evidence_kind": evidence_kind,
                        **json.loads(payload),
                    }
                    for seq, cat, ts, version, payload, entity, tool, outcome, evidence_kind in rows
                ],
            }

    async def recent_choices(self, state, limit=100):
        with self.lock:
            incident = self.history_scope(state)
            rows = self.connection.execute(
                "SELECT sequence,timestamp,payload FROM history_entries WHERE incident_id=? AND category='choice' ORDER BY sequence DESC LIMIT ?",
                (incident, limit),
            ).fetchall()
        return [
            {
                "event_ref": f"{incident}:{seq}",
                "timestamp": timestamp,
                **json.loads(payload),
            }
            for seq, timestamp, payload in rows
        ]

    async def action_templates(self, state, tool):
        """Logical argument templates only; no history payloads enter runtime state."""
        with self.lock:
            incident = self.history_scope(state)
            rows = self.connection.execute(
                "SELECT DISTINCT json_remove(args,'$.revision') FROM memory_actions WHERE incident_id=? AND tool=? AND version=? AND json_extract(args,'$.operation') IN ('inspect','next')",
                (incident, tool, state.payload.get("index_fingerprint", "")),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

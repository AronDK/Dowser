"""Recoverable append-only mirror of SQLite events. Never executes tools."""

import fcntl
import hashlib
import json
import os
import warnings
from pathlib import Path


def encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


class AuditJournal:
    def initialize_journal(self, path):
        self.journal_path = Path(path) if path else None
        self.journal_error = None
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS audit_outbox(id INTEGER PRIMARY KEY,incident_id TEXT NOT NULL,sequence INTEGER NOT NULL,UNIQUE(incident_id,sequence))"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS audit_mirror_state(path TEXT PRIMARY KEY,sequence INTEGER,offset INTEGER,start INTEGER,sha TEXT,device INTEGER,inode INTEGER,error TEXT)"
        )
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO audit_outbox(incident_id,sequence) SELECT incident_id,sequence FROM events ORDER BY timestamp,incident_id,sequence"
            )
        if self.journal_path:
            self.flush_journal()

    def journal_enqueue(self, incident, sequence):
        # Outbox exists even while mirroring is disabled; later export can recover.
        self.connection.execute(
            "INSERT INTO audit_outbox(incident_id,sequence) VALUES(?,?)",
            (incident, sequence),
        )

    def journal_artifact(self, data):
        raw = encoded(data)
        sha = hashlib.sha256(raw).hexdigest()
        directory = self.journal_path.parent / (self.journal_path.name + ".artifacts")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (sha + ".json")
        if path.exists():
            if path.read_bytes() != raw:
                raise ValueError("journal artifact integrity failure")
        else:
            temporary = directory / (sha + ".tmp")
            with temporary.open("wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        return {
            "artifact": str(path.relative_to(self.journal_path.parent)),
            "sha256": sha,
            "bytes": len(raw),
        }

    def journal_record(self, journal_id, incident, sequence, previous):
        row = self.connection.execute(
            "SELECT schema_version,timestamp,kind,payload FROM events WHERE incident_id=? AND sequence=?",
            (incident, sequence),
        ).fetchone()
        if not row:
            raise ValueError("outbox event missing")
        value = json.loads(row[3])
        payload = self.journal_artifact(value) if len(encoded(value)) > 65536 else value
        artifacts = {
            ref: self.journal_artifact(json.loads(data))
            for ref, data in self.connection.execute(
                "SELECT ref,payload FROM artifacts WHERE incident_id=? AND sequence=? ORDER BY ref",
                (incident, sequence),
            )
        }
        result = {
            "journal_sequence": journal_id,
            "incident_id": incident,
            "sequence": sequence,
            "schema_version": row[0],
            "timestamp": row[1],
            "kind": row[2],
            "payload": payload,
            "artifacts": artifacts,
            "previous_sha256": previous,
        }
        result["sha256"] = hashlib.sha256(encoded(result)).hexdigest()
        return result

    def repair_journal(self, path=None):
        with self.lock:
            if path:
                self.journal_path = Path(path)
            if not self.journal_path:
                raise ValueError("journal path required")
            self.journal_path.parent.mkdir(parents=True, exist_ok=True)
            recovered, previous, last, last_start = 0, "", 0, 0
            # Verify the existing prefix against authoritative SQLite; only a torn
            # final line is truncated. Complete corrupt lines must not be hidden.
            with self.journal_path.open("a+b") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                stream.seek(0)
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        stream.truncate(start)
                        break
                    record = json.loads(line)
                    jid = record["journal_sequence"]
                    if jid != last + 1:
                        raise ValueError("journal sequence gap")
                    row = self.connection.execute(
                        "SELECT incident_id,sequence FROM audit_outbox WHERE id=?",
                        (jid,),
                    ).fetchone()
                    if not row or record != self.journal_record(jid, *row, previous):
                        raise ValueError(
                            "journal prefix differs from authoritative history"
                        )
                    previous, last, last_start = record["sha256"], jid, start
                for jid, incident, sequence in self.connection.execute(
                    "SELECT id,incident_id,sequence FROM audit_outbox WHERE id>? ORDER BY id",
                    (last,),
                ):
                    start = stream.tell()
                    record = self.journal_record(jid, incident, sequence, previous)
                    stream.write(encoded(record) + b"\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                    previous, last, last_start = record["sha256"], jid, start
                    recovered += 1
                self.checkpoint_journal(stream, last, previous, last_start)
            self.journal_error = None
            self.journal_cursor = last
            self.journal_sha = previous
            return {
                "path": str(self.journal_path),
                "sequence": last,
                "recovered": recovered,
                "integrity": "verified",
            }

    def checkpoint_journal(self, stream, sequence, sha, start):
        stat = os.fstat(stream.fileno())
        self.journal_offset = stat.st_size
        self.journal_last_start = start
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO audit_mirror_state VALUES(?,?,?,?,?,?,?,NULL)",
                (
                    str(self.journal_path.absolute()),
                    sequence,
                    stat.st_size,
                    start,
                    sha,
                    stat.st_dev,
                    stat.st_ino,
                ),
            )

    def resume_journal(self):
        row = self.connection.execute(
            "SELECT sequence,offset,start,sha,device,inode FROM audit_mirror_state WHERE path=? AND error IS NULL",
            (str(self.journal_path.absolute()),),
        ).fetchone()
        if not row or not self.journal_path.is_file():
            return False
        with self.journal_path.open("rb") as stream:
            fcntl.flock(stream, fcntl.LOCK_SH)
            stat = os.fstat(stream.fileno())
            if (stat.st_size, stat.st_dev, stat.st_ino) != (row[1], row[4], row[5]):
                return False
            stream.seek(row[2])
            line = stream.readline()
            if row[0]:
                if not line.endswith(b"\n"):
                    return False
                record = json.loads(line)
                event = self.connection.execute(
                    "SELECT incident_id,sequence FROM audit_outbox WHERE id=?",
                    (row[0],),
                ).fetchone()
                if (
                    not event
                    or record
                    != self.journal_record(row[0], *event, record["previous_sha256"])
                    or record["sha256"] != row[3]
                ):
                    return False
        (
            self.journal_cursor,
            self.journal_offset,
            self.journal_last_start,
            self.journal_sha,
        ) = row[:4]
        return True

    def flush_journal(self):
        if not self.journal_path:
            return
        try:
            # A FULL-durable checkpoint avoids rescanning every previous trial on
            # normal reopen. Explicit repair verifies the complete hash chain.
            if not hasattr(self, "journal_cursor") and not self.resume_journal():
                self.repair_journal()
                return
            with self.journal_path.open("a+b") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                stream.seek(0, os.SEEK_END)
                if stream.tell() != self.journal_offset:
                    raise ValueError("journal changed concurrently; repair required")
                start = self.journal_last_start
                for jid, incident, sequence in self.connection.execute(
                    "SELECT id,incident_id,sequence FROM audit_outbox WHERE id>? ORDER BY id",
                    (self.journal_cursor,),
                ):
                    start = stream.tell()
                    record = self.journal_record(
                        jid, incident, sequence, self.journal_sha
                    )
                    stream.write(encoded(record) + b"\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                    self.journal_cursor, self.journal_sha = jid, record["sha256"]
                self.checkpoint_journal(
                    stream, self.journal_cursor, self.journal_sha, start
                )
        except Exception as exc:
            self.journal_error = {
                "error_type": type(exc).__name__,
                "repair_required": True,
            }
            if hasattr(self, "journal_cursor"):
                del self.journal_cursor
            with self.connection:
                self.connection.execute(
                    "INSERT INTO audit_mirror_state(path,error) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET error=excluded.error",
                    (str(self.journal_path.absolute()), json.dumps(self.journal_error)),
                )
            warnings.warn(
                "Audit mirror failed; SQLite is durable. Run journal-repair.",
                RuntimeWarning,
                stacklevel=2,
            )

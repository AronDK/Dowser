"""Immutable ITBench snapshots and a separate, evidence-only SQLite index."""

import ast
import csv
import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

REVISION = "76df38a82288f75ba9e41dc8c515033332497473"
REPO = "ArtificialAnalysis/ITBench-AA"
TITLE = "ITBench-AA public subset — Dowser/Jev adapted evaluation"
PILOT = [8, 2, 19, 17, 16, 9, 7, 6, 31, 102]
INDEX_VERSION = 2
PAGE_BYTES = 4096
STATE_BYTES = 8192
CLUSTER_KINDS = {
    "Node",
    "Namespace",
    "PersistentVolume",
    "ClusterRole",
    "ClusterRoleBinding",
    "CustomResourceDefinition",
    "StorageClass",
}
EVIDENCE_FILES = {
    "k8s_objects_raw.tsv": "configuration",
    "k8s_events_raw.tsv": "events",
    "otel_logs_raw.tsv": "logs",
    "otel_traces_raw.tsv": "traces",
}
FORBIDDEN = re.compile(
    r"password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|credentials|private[_-]?key",
    re.I,
)
LABEL_FIELDS = {
    "ground_truth",
    "ground_truth_yaml",
    "recommended_actions",
    "recommended_remediations",
    "grader_results",
    "root_cause",
    "propagations",
}


class ContextOverflowError(ValueError):
    """Required benchmark evidence or state cannot fit its declared byte limit."""


def dumps(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def confined(root, relative, *, must_exist=True):
    """Reject traversal and all symlinks, including an ancestor symlink."""
    root = Path(root).absolute()
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError("path outside authorized root")
    path = root / rel
    if any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("symlink access is forbidden")
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("path outside authorized root")
    if must_exist and not path.is_file():
        raise ValueError("required file missing")
    return path


def atomic_json(path, value):
    path = Path(path)
    confined(path.parent, path.name, must_exist=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = confined(path.parent, path.name + ".tmp", must_exist=False)
    with temp.open("w") as handle:
        handle.write(dumps(value) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temp.replace(path)


def sanitize(value):
    """Keep configuration facts, redact credentials and deny label-bearing fields."""
    if isinstance(value, dict):
        result = {}
        sensitive_env = (
            FORBIDDEN.search(str(value.get("name", ""))) and "value" in value
        )
        for key, child in value.items():
            if key.lower() in LABEL_FIELDS:
                continue
            if FORBIDDEN.search(key) or (sensitive_env and key == "value"):
                result["redacted_" + hashlib.sha256(key.encode()).hexdigest()[:8]] = (
                    "[REDACTED]"
                )
            elif key == "data" and value.get("kind") == "Secret":
                result[key] = "[REDACTED]"
            else:
                result[key] = sanitize(child)
        return result
    if isinstance(value, list):
        return [sanitize(x) for x in value]
    if isinstance(value, str):
        parsed = decoded(value)
        if parsed is not value:
            return sanitize(parsed)
        value = re.sub(r"(?i)Bearer\s+\S+", "[REDACTED]", value)
        value = re.sub(
            r"(?i)((?:password|passwd|api[_-]?key|access[_-]?token|authorization)\s*[=:]\s*)[^\s,;]+",
            "[REDACTED]",
            value,
        )
        return value
    return value


@lru_cache(maxsize=128)
def parse_text(value):
    try:
        return json.loads(value)
    except ValueError:
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError, RecursionError):
            return value


def decoded(value):
    if isinstance(value, str):
        if value.lstrip().startswith(("{", "[")):
            return parse_text(value)
    return value


def walk(value):
    value = decoded(value)
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def identity(namespace, kind, name):
    if not all(isinstance(x, str) and x and "/" not in x for x in (kind, name)):
        return None
    namespace = "cluster" if kind in CLUSTER_KINDS else namespace
    if not isinstance(namespace, str) or not namespace or "/" in namespace:
        return None
    return f"{namespace}/{kind}/{name}"


def object_id(obj):
    meta = obj.get("metadata", {})
    return (
        identity(meta.get("namespace"), obj.get("kind"), meta.get("name"))
        if isinstance(meta, dict)
        else None
    )


def row_identities(row):
    result = set()
    for obj in walk(row):
        if key := object_id(obj):
            result.add(key)
        # Events and telemetry carry identities without an entire object.
        if key := identity(obj.get("namespace"), obj.get("kind"), obj.get("name")):
            result.add(key)
        for kind, attr in (
            ("Service", "service_name"),
            ("Pod", "pod_name"),
            ("Pod", "pod"),
        ):
            if key := identity(obj.get("namespace"), kind, obj.get(attr)):
                result.add(key)
        for kind, attr in (
            ("Pod", "k8s.pod.name"),
            ("Deployment", "k8s.deployment.name"),
            ("Service", "k8s.service.name"),
            ("Node", "k8s.node.name"),
        ):
            if key := identity(obj.get("k8s.namespace.name"), kind, obj.get(attr)):
                result.add(key)
    return result


def primary_objects(row):
    """Only actual objects, never identities mentioned inside their spec/status."""
    value = decoded(row)
    if isinstance(value, dict):
        if object_id(value):
            yield value
        else:
            for child in value.values():
                yield from primary_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from primary_objects(child)


def evidence_owners(row, kind):
    if kind == "configuration":
        return {object_id(obj) for obj in primary_objects(row)}
    if kind == "events":
        targets = set()
        for obj in walk(row):
            for field in ("regarding", "involvedObject"):
                target = decoded(obj.get(field))
                if isinstance(target, dict):
                    key = identity(
                        target.get("namespace"), target.get("kind"), target.get("name")
                    )
                    if key:
                        targets.add(key)
        return targets
    return row_identities(row)


def compact_object(obj):
    # Resource versions, timestamps and field-manager churn are provenance, not findings.
    result = {
        "kind": obj["kind"],
        "metadata": {
            k: v
            for k, v in obj.get("metadata", {}).items()
            if k in {"name", "namespace", "labels", "annotations", "ownerReferences"}
        },
    }
    for key in ("spec", "data", "binaryData", "status"):
        if key in obj:
            result[key] = obj[key]
    return strip_administration(result)


def strip_administration(value):
    if isinstance(value, dict):
        return {
            k: strip_administration(v)
            for k, v in value.items()
            if k
            not in {
                "managedFields",
                "resourceVersion",
                "creationTimestamp",
                "lastTransitionTime",
                "lastProbeTime",
                "kubectl.kubernetes.io/last-applied-configuration",
            }
        }
    if isinstance(value, list):
        return [strip_administration(v) for v in value]
    return value


def compact_evidence(data, entity, kind):
    if kind in {"configuration", "history"}:
        return {
            "objects": [
                compact_object(obj)
                for obj in primary_objects(data)
                if object_id(obj) == entity
            ]
        }
    if kind == "events":
        return {
            "events": [
                {
                    k: v
                    for k, v in obj.items()
                    if k
                    in {
                        "reason",
                        "message",
                        "note",
                        "type",
                        "regarding",
                        "involvedObject",
                        "count",
                        "series",
                        "action",
                        "reportingController",
                    }
                }
                for obj in walk(data)
                if "regarding" in obj or "involvedObject" in obj
            ]
        }
    return strip_administration(data)


def snapshot_files(root):
    files = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise ValueError("snapshot symlink forbidden")
        if not path.is_file():
            continue
        if relative in EVIDENCE_FILES:
            files.append((path, relative, EVIDENCE_FILES[relative]))
        elif re.fullmatch(r"metrics/(pod|service)_[^/]+_raw\.tsv", relative):
            files.append((path, relative, "metrics"))
        elif re.fullmatch(
            r"(?:alerts/)?alerts_(at_|in_alerting_state_)[^/]+\.json", relative
        ):
            files.append((path, relative, "alerts"))
    return files


def tsv_rows(path):
    csv.field_size_limit(128 * 1024 * 1024)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(
            reader.fieldnames
        ):
            raise ValueError("invalid TSV header")
        for record, row in enumerate(reader, 1):
            if None in row or any(v is None for v in row.values()):
                raise ValueError("invalid TSV record")
            yield record, row


def json_rows(path):
    data = json.loads(path.read_text())
    if isinstance(data, dict):
        data = data.get("data", data)
    if isinstance(data, dict):
        data = data.get("alerts", [data])
    if not isinstance(data, list):
        raise ValueError("unsupported alerts layout")
    yield from enumerate(data, 1)


def object_dependencies(obj):
    child = object_id(obj)
    if not child:
        return []
    meta, spec = obj.get("metadata", {}), obj.get("spec", {})
    links = []
    node = identity(None, "Node", spec.get("nodeName"))
    if node:
        links.append((child, node, "placement"))
    for part in walk(spec):
        for field in ("configMapRef", "configMapKeyRef", "configMap"):
            ref = part.get(field, {})
            if isinstance(ref, dict):
                target = identity(meta.get("namespace"), "ConfigMap", ref.get("name"))
                if target:
                    links.append((child, target, "configuration"))
    return links


def upgrade_index(source, output, scenario, expected_files):
    """Reuse immutable sanitized rows; rebuild ownership in a distinct v2 file."""
    import shutil

    index = EvidenceIndex(source, scenario)
    if index.meta["files"] != expected_files:
        raise ValueError("source index differs from pinned evidence files")
    output = Path(output)
    if output.exists() or output.absolute() == Path(source).absolute():
        raise ValueError("index already exists; prepare in a separate root")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = confined(output.parent, output.name + ".building", must_exist=False)
    # Source is opened read-only; a standalone copy never shares mutable pages.
    shutil.copyfile(index.path, temp)
    db = sqlite3.connect(temp)
    try:
        db.executescript(
            "DROP TABLE IF EXISTS record_owners; CREATE TABLE record_owners(record_id INTEGER,entity TEXT,PRIMARY KEY(record_id,entity));"
        )
        db.execute(
            "INSERT OR IGNORE INTO record_owners SELECT e.record_id,e.entity FROM record_entities e JOIN records r ON r.id=e.record_id WHERE r.kind NOT IN ('configuration','events')"
        )
        for rid, kind, payload in db.execute(
            "SELECT id,kind,payload FROM records WHERE kind IN ('configuration','events')"
        ):
            row = json.loads(payload)
            for owner in evidence_owners(row, kind):
                db.execute("INSERT OR IGNORE INTO entities VALUES(?)", (owner,))
                db.execute(
                    "INSERT OR IGNORE INTO record_owners VALUES(?,?)", (rid, owner)
                )
                db.execute(
                    "INSERT OR IGNORE INTO record_entities VALUES(?,?)", (rid, owner)
                )
            if kind == "configuration":
                for obj in primary_objects(row):
                    for child, target, relation in object_dependencies(obj):
                        db.execute(
                            "INSERT OR IGNORE INTO entities VALUES(?)", (target,)
                        )
                        db.execute(
                            "INSERT OR IGNORE INTO relationships VALUES(?,?,?)",
                            (child, target, relation),
                        )
        db.execute("CREATE INDEX owner_entity ON record_owners(entity,record_id)")
        meta = {k: v for k, v in index.meta.items() if k != "fingerprint"}
        meta["version"] = INDEX_VERSION
        meta["fingerprint"] = fingerprint(meta)
        db.execute("UPDATE meta SET value=? WHERE key='manifest'", (dumps(meta),))
        db.commit()
    finally:
        db.close()
    temp.replace(output)
    return meta


def create_index(root, output, scenario):
    """One record at a time; originals never modified. Labels are not read here."""
    root, output = Path(root), Path(output)
    files = snapshot_files(root)
    if output.exists():
        raise ValueError("index already exists; prepare in a separate root")
    if not any(kind == "configuration" for _, _, kind in files):
        raise ValueError("snapshot lacks object history")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = confined(output.parent, output.name + ".building", must_exist=False)
    if temp.exists():
        temp.unlink()
    db = sqlite3.connect(temp)
    db.executescript("""
        CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE entities(name TEXT PRIMARY KEY);
        CREATE TABLE records(id INTEGER PRIMARY KEY,kind TEXT,file TEXT,record INTEGER,timestamp TEXT,payload TEXT);
        CREATE TABLE record_entities(record_id INTEGER,entity TEXT,PRIMARY KEY(record_id,entity));
        CREATE TABLE record_owners(record_id INTEGER,entity TEXT,PRIMARY KEY(record_id,entity));
        CREATE TABLE relationships(source TEXT,target TEXT,relation TEXT,PRIMARY KEY(source,target,relation));
    """)
    hashes, counts = {}, {}
    try:
        for path, relative, kind in files:
            confined(root, relative)
            hashes[relative] = file_hash(path)
            counts[kind] = counts.get(kind, 0)
            rows = tsv_rows(path) if path.suffix == ".tsv" else json_rows(path)
            for record, raw in rows:
                row = sanitize(raw)
                timestamp = (
                    str(next((v for k, v in row.items() if "time" in k.lower()), ""))
                    if isinstance(row, dict)
                    else ""
                )
                cursor = db.execute(
                    "INSERT INTO records(kind,file,record,timestamp,payload) VALUES(?,?,?,?,?)",
                    (kind, relative, record, timestamp, dumps(row)),
                )
                owners = evidence_owners(raw, kind)
                ids = row_identities(raw) | owners
                for key in owners:
                    db.execute(
                        "INSERT OR IGNORE INTO record_owners VALUES(?,?)",
                        (cursor.lastrowid, key),
                    )
                for key in ids:
                    db.execute("INSERT OR IGNORE INTO entities VALUES(?)", (key,))
                    db.execute(
                        "INSERT OR IGNORE INTO record_entities VALUES(?,?)",
                        (cursor.lastrowid, key),
                    )
                for obj in walk(raw):
                    child = object_id(obj)
                    if child:
                        meta = obj["metadata"]
                        for owner in meta.get("ownerReferences", []):
                            parent = identity(
                                meta.get("namespace"),
                                owner.get("kind"),
                                owner.get("name"),
                            )
                            if parent:
                                db.execute(
                                    "INSERT OR IGNORE INTO entities VALUES(?)",
                                    (parent,),
                                )
                                db.execute(
                                    "INSERT OR IGNORE INTO relationships VALUES(?,?,?)",
                                    (child, parent, "owner"),
                                )
                    for child, target, relation in object_dependencies(obj):
                        db.execute(
                            "INSERT OR IGNORE INTO entities VALUES(?)", (target,)
                        )
                        db.execute(
                            "INSERT OR IGNORE INTO relationships VALUES(?,?,?)",
                            (child, target, relation),
                        )
                counts[kind] += 1
                if counts[kind] % 5000 == 0:
                    db.commit()
            db.commit()
        # Service selectors explicitly link observed services and observed pods.
        pods, services = {}, {}
        for (payload,) in db.execute(
            "SELECT payload FROM records WHERE kind='configuration'"
        ):
            for obj in walk(json.loads(payload)):
                key = object_id(obj)
                if key and obj.get("kind") == "Pod":
                    pods[key] = obj.get("metadata", {}).get("labels", {})
                if key and obj.get("kind") == "Service":
                    services[key] = obj.get("spec", {}).get("selector", {})
        for service, selector in services.items():
            for pod, labels in pods.items():
                if (
                    selector
                    and service.split("/")[0] == pod.split("/")[0]
                    and all(labels.get(k) == v for k, v in selector.items())
                ):
                    db.execute(
                        "INSERT OR IGNORE INTO relationships VALUES(?,?,?)",
                        (pod, service, "selector"),
                    )
        db.executescript(
            "CREATE INDEX records_kind ON records(kind,id); CREATE INDEX evidence_entity ON record_entities(entity,record_id); CREATE INDEX owner_entity ON record_owners(entity,record_id);"
        )
        metadata = {
            "version": INDEX_VERSION,
            "scenario": scenario,
            "files": hashes,
            "counts": counts,
        }
        metadata["fingerprint"] = fingerprint(metadata)
        db.execute("INSERT INTO meta VALUES('manifest',?)", (dumps(metadata),))
        db.commit()
    finally:
        db.close()
    temp.replace(output)
    return metadata


class EvidenceIndex:
    def __init__(self, path, scenario):
        path = confined(Path(path).parent, Path(path).name)
        self.path = path
        self.scenario = scenario
        with self.connect() as db:
            self.meta = json.loads(
                db.execute("SELECT value FROM meta WHERE key='manifest'").fetchone()[0]
            )
        if self.meta["scenario"] != scenario or self.meta["version"] not in {
            1,
            INDEX_VERSION,
        }:
            raise ValueError("index outside scenario scope or version")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        try:
            yield db
        finally:
            db.close()

    def entities(self):
        with self.connect() as db:
            return [r[0] for r in db.execute("SELECT name FROM entities ORDER BY name")]

    def relationships(self):
        with self.connect() as db:
            return list(
                db.execute(
                    "SELECT source,target,relation FROM relationships ORDER BY source,target"
                )
            )

    @lru_cache(maxsize=256)
    def refs(self, entity, kind, related=False):
        with self.connect() as db:
            table = (
                "record_entities"
                if related or self.meta["version"] == 1
                else "record_owners"
            )
            if entity:
                rows = db.execute(
                    f"SELECT r.id FROM records r JOIN {table} e ON r.id=e.record_id WHERE e.entity=? AND r.kind=? ORDER BY r.id DESC",
                    (entity, kind),
                )
            else:
                rows = db.execute(
                    "SELECT id FROM records WHERE kind=? ORDER BY id DESC", (kind,)
                )
            refs = [r[0] for r in rows]
        if entity and self.meta["version"] == 1 and not related:
            refs = [
                rid
                for rid in refs
                if entity in evidence_owners(self.record(rid)["data"], kind)
            ]
        if related and entity:
            refs = [rid for rid in refs if not self.owns(entity, rid)]
        return refs

    def record(self, record_id):
        with self.connect() as db:
            row = db.execute(
                "SELECT kind,file,record,timestamp,payload FROM records WHERE id=?",
                (record_id,),
            ).fetchone()
        if not row:
            raise ValueError("unknown evidence record")
        return {
            "ref": f"Scenario-{self.scenario}:{row[1]}:record-{row[2]}",
            "kind": row[0],
            "timestamp": row[3],
            "data": json.loads(row[4]),
        }

    @lru_cache(maxsize=4096)
    def owns(self, entity, record_id):
        if self.meta["version"] == 1:
            record = self.record(record_id)
            return entity in evidence_owners(record["data"], record["kind"])
        with self.connect() as db:
            return (
                db.execute(
                    "SELECT 1 FROM record_owners WHERE entity=? AND record_id=?",
                    (entity, record_id),
                ).fetchone()
                is not None
            )

    @lru_cache(maxsize=512)
    def revisions(self, entity):
        groups = []
        previous = None
        for rid in self.refs(entity, "configuration"):
            record = self.record(rid)
            projection = compact_evidence(record["data"], entity, "configuration")
            semantic = fingerprint(projection)
            if semantic == previous:
                groups[-1]["count"] += 1
                groups[-1]["oldest_ref"] = record["ref"]
            else:
                groups.append(
                    {
                        "id": rid,
                        "count": 1,
                        "oldest_ref": record["ref"],
                        "semantic": semantic,
                    }
                )
            previous = semantic
        return groups

    def page(self, entity, kind, offset=0, segment=0):
        raw_view = kind == "raw" or kind.startswith("raw_")
        view_kind = kind.removeprefix("raw_")
        related = view_kind.startswith("related_")
        source_kind = view_kind.removeprefix("related_")
        source_kind = (
            "configuration" if source_kind in {"history", "raw"} else source_kind
        )
        groups = (
            self.revisions(entity) if kind in {"configuration", "history"} else None
        )
        refs = (
            [g["id"] for g in groups]
            if groups is not None
            else self.refs(entity, source_kind, related)
        )
        if offset >= len(refs):
            return {
                "records": [],
                "next": None,
                "missing_evidence": True,
                "entity": entity,
            }
        rid = refs[offset]
        full = self.record(rid)
        semantic = fingerprint(compact_evidence(full["data"], entity, source_kind))
        if not raw_view:
            # Related records are explicitly labelled; their primary owners retain authority.
            full["data"] = (
                compact_evidence(full["data"], entity, source_kind)
                if not related
                else strip_administration(full["data"])
            )
        # Byte-bounded segments keep even huge object revisions accessible.
        raw = dumps(full["data"]).encode()
        chunks, start = [], 0
        while start < len(raw):
            end = min(start + 2800, len(raw))
            while end < len(raw) and raw[end] & 0xC0 == 0x80:
                end -= 1
            chunks.append(raw[start:end].decode())
            start = end
        if not 0 <= segment < len(chunks):
            raise ValueError("invalid evidence segment")
        nxt = (
            [offset, segment + 1]
            if segment + 1 < len(chunks)
            else ([offset + 1, 0] if offset + 1 < len(refs) else None)
        )
        page = {
            "records": [
                {
                    "id": rid,
                    "ref": full["ref"],
                    "kind": kind,
                    "timestamp": full["timestamp"],
                    "segment": segment,
                    "segments": len(chunks),
                    "content": chunks[segment],
                    "ownership": "related" if related else "primary",
                    "semantic_fingerprint": semantic,
                    "empty_view": source_kind == "configuration"
                    and not any(
                        any(
                            obj.get(k) for k in ("spec", "data", "binaryData", "status")
                        )
                        for obj in primary_objects(self.record(rid)["data"])
                        if object_id(obj) == entity
                    ),
                    "collapsed_revisions": groups[offset]["count"] if groups else 1,
                    "oldest_ref": groups[offset]["oldest_ref"]
                    if groups
                    else full["ref"],
                }
            ],
            "next": nxt,
            "total_records": len(refs),
            "source_records": sum(g["count"] for g in groups) if groups else len(refs),
        }
        if len(dumps(page).encode()) > PAGE_BYTES:
            raise ContextOverflowError("context overflow: evidence page")
        return page

    def alert_summaries(self):
        summaries = {}
        for rid in self.refs(None, "alerts"):
            row = self.record(rid)["data"]
            labels = row.get("labels", {})
            annotations = row.get("annotations", {})
            summary = {
                "alert": labels.get("alertname", row.get("name", "alert")),
                "namespace": labels.get("namespace", ""),
                "service": labels.get("service_name", labels.get("service", "")),
                "causal_shortlist": labels.get("alertname")
                not in {"Watchdog", "InfoInhibitor"},
                "entities": sorted(
                    row_identities(row)
                    | {
                        key
                        for kind, attr in (
                            ("Service", "service"),
                            ("Deployment", "deployment"),
                            ("Pod", "pod"),
                            ("Node", "node"),
                            ("ConfigMap", "configmap"),
                        )
                        if (
                            key := identity(
                                labels.get("namespace"), kind, labels.get(attr)
                            )
                        )
                    }
                ),
                "summary": annotations.get(
                    "summary", annotations.get("description", "")
                ),
            }
            key = (summary["alert"], summary["namespace"], summary["service"])
            if row.get("state") == "firing" and key not in summaries:
                summary["evidence_ref"] = self.record(rid)["ref"]
                summaries[key] = summary
        return [summaries[k] for k in sorted(summaries)]

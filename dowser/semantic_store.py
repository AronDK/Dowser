"""Durable SQLite evidence with a disposable, incident-isolated RAM retrieval index."""

import asyncio
import copy
import json
import math
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import Field, model_validator

from .bench_ledger import Ledger
from .contracts import factory
from .diagnostics import DiagnosticError
from .memory import digest
from .models import MemoryQueryIntent
from .rate_limit import UnlimitedRateLimiter, shared_rate_limiter
from .runtime import call_deadline
from .semantic import (
    RENDERER_VERSION,
    Document,
    document_texts,
    query_intents,
    rank,
    relationships,
)
from .store import SQLiteStore, StoreSettings, reject_credentials
from .tokens import estimate


class EmbeddingUnavailable(DiagnosticError):
    """A transient or interrupted embedding request; absence is never inferred."""


class SemanticSettings(StoreSettings):
    mode: Literal["deterministic", "vector", "graph"] = "graph"
    embedding_model: Literal["text-embedding-3-small"] = "text-embedding-3-small"
    dimensions: int = Field(default=1536, ge=1, le=1536)
    embedding_rpm: int | None = Field(default=None, ge=1)
    embedding_tpm: int | None = Field(default=None, ge=1)
    unlimited_account_limits: bool = False
    # Optional combined account constraints, in addition to endpoint limits.
    account_rpm: int | None = Field(default=None, ge=1)
    account_tpm: int | None = Field(default=None, ge=1)
    env_file: str | None = ".env"
    credential_env: Literal["OPENAI_API_KEY"] = "OPENAI_API_KEY"
    ledger: str | None = None
    input_price: int = Field(default=20, ge=0)
    attempt_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    max_retries: int = Field(default=2, ge=0, le=5)
    max_queries: int = Field(default=8, ge=1, le=32)
    chunk_tokens: int = Field(default=1024, ge=128, le=8191)
    memory_tokens: int = Field(default=16384, ge=1024)
    max_documents: int = Field(default=24, ge=1, le=200)
    per_query: int = Field(default=20, ge=1, le=200)

    @model_validator(mode="after")
    def paired_limits(self):
        if self.unlimited_account_limits and any(
            value is not None
            for value in (
                self.embedding_rpm,
                self.embedding_tpm,
                self.account_rpm,
                self.account_tpm,
            )
        ):
            raise ValueError("unlimited admission cannot also specify account caps")
        for rpm, tpm in (
            (self.embedding_rpm, self.embedding_tpm),
            (self.account_rpm, self.account_tpm),
        ):
            if (rpm is None) != (tpm is None):
                raise ValueError("RPM and TPM must be configured together")
        return self


class OpenAIEmbedder:
    """SDK retries disabled: each explicit attempt has its own durable reservation."""

    def __init__(self, settings, store, base):
        self.settings, self.store, self.base = settings, store, base

    def credential(self):
        settings = self.settings
        if settings.env_file:
            from dotenv import load_dotenv

            load_dotenv(self.base / settings.env_file, override=False)
        key = os.environ.get(settings.credential_env)
        if not key:
            raise DiagnosticError(
                "Embedding credential unavailable",
                code="missing_credential",
                category="configuration",
                stage="credentials",
            )
        return key

    async def embed(self, texts, incident):
        from openai import AsyncOpenAI

        settings = self.settings
        if not settings.unlimited_account_limits and (
            settings.embedding_rpm is None or settings.embedding_tpm is None
        ):
            raise DiagnosticError(
                "Configure embedding RPM/TPM",
                code="embedding_limits_missing",
                category="configuration",
                stage="preparation",
            )
        key = self.credential()
        if any(key in text for text in texts):
            raise DiagnosticError(
                "Credential in embedding input",
                code="credential_in_input",
                category="configuration",
                stage="preparation",
            )
        estimates = [estimate(text, settings.embedding_model) for text in texts]
        tokens = sum(e["estimated_tokens"] for e in estimates)
        if (
            not texts
            or any(e["estimated_tokens"] > 8192 for e in estimates)
            or tokens > 300000
        ):
            raise ValueError("embedding request exceeds documented input limits")
        deadline = call_deadline.get()
        limiter = (
            UnlimitedRateLimiter()
            if settings.unlimited_account_limits
            else shared_rate_limiter(
                ("embeddings", digest(key)),
                settings.embedding_rpm,
                settings.embedding_tpm,
                window_seconds=60,
            )
        )
        account = None
        if settings.account_rpm is not None and settings.account_tpm is not None:
            account = shared_rate_limiter(
                ("openai-account", digest(key)),
                settings.account_rpm,
                settings.account_tpm,
                window_seconds=60,
            )
        ledger = Ledger(Path(settings.ledger)) if settings.ledger else None
        request_id = str(uuid4())
        for attempt in range(1, settings.max_retries + 2):
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("incident deadline exhausted")
            ticket = await limiter.acquire(tokens, deadline)
            account_ticket = (
                await account.acquire(tokens, deadline) if account else None
            )
            call_id = (
                ledger.reserve(
                    incident,
                    attempt=attempt,
                    request_id=request_id,
                    tokens=tokens,
                    token_ceiling=300000,
                    provider="openai",
                    model=settings.embedding_model,
                    endpoint="/v1/embeddings",
                    input_price=settings.input_price,
                )
                if ledger
                else None
            )
            await self.store.append(
                incident,
                "embedding_request",
                {
                    "request_id": request_id,
                    "attempt": attempt,
                    "endpoint": "/v1/embeddings",
                    "model": settings.embedding_model,
                    "dimensions": settings.dimensions,
                    "renderer": RENDERER_VERSION,
                    "input": texts,
                    "estimate": {
                        "estimated_tokens": tokens,
                        "methods": sorted({e["method"] for e in estimates}),
                        "exact": False,
                    },
                    "call_id": call_id,
                    "admission_seconds": ticket.waited
                    + (account_ticket.waited if account_ticket else 0),
                },
                None,
            )
            started = time.monotonic()
            try:
                seconds = (
                    min(settings.attempt_seconds, deadline - time.monotonic())
                    if deadline
                    else settings.attempt_seconds
                )
                async with AsyncOpenAI(
                    api_key=key, max_retries=0, timeout=max(0.001, seconds)
                ) as client:
                    async with asyncio.timeout(seconds):
                        response = await client.embeddings.create(
                            model=settings.embedding_model,
                            dimensions=settings.dimensions,
                            encoding_format="float",
                            input=texts,
                        )
                value = response.model_dump(mode="json")
                # Persist the received response before interpreting vector data.
                await self.store.append(
                    incident,
                    "embedding_response",
                    {
                        "request_id": request_id,
                        "attempt": attempt,
                        "response": value,
                        "seconds": time.monotonic() - started,
                    },
                    None,
                )
                usage = value.get("usage", {})
                actual = usage.get("prompt_tokens")
                if (
                    type(actual) is not int
                    or actual < 0
                    or usage.get("total_tokens") != actual
                    or value.get("model") != settings.embedding_model
                ):
                    raise DiagnosticError(
                        "Malformed embedding usage",
                        code="malformed_embedding_response",
                        category="response",
                        stage="validation",
                    )
                if ledger:
                    ledger.reconcile(
                        call_id,
                        {"input_tokens": actual, "output_tokens": 0},
                        time.monotonic() - started,
                    )
                limiter.reconcile(ticket, actual)
                if account_ticket:
                    account.reconcile(account_ticket, actual)
                entries = value.get("data", [])
                if len(entries) != len(texts) or sorted(
                    e.get("index", -1) for e in entries
                ) != list(range(len(texts))):
                    raise ValueError("invalid embedding indexes")
                return [
                    validate_vector(e["embedding"], settings.dimensions)
                    for e in sorted(entries, key=lambda e: e["index"])
                ]
            except BaseException as error:
                status = getattr(error, "status_code", None)
                transient = (
                    isinstance(error, TimeoutError)
                    or type(error).__name__ in {"APIConnectionError", "APITimeoutError"}
                    or status in {408, 429}
                    or (isinstance(status, int) and status >= 500)
                )
                if ledger:
                    detail = (
                        error
                        if isinstance(
                            error,
                            (
                                DiagnosticError,
                                asyncio.CancelledError,
                                KeyboardInterrupt,
                            ),
                        )
                        else DiagnosticError(
                            "Embedding attempt failed",
                            code="embedding_attempt_timeout"
                            if isinstance(error, TimeoutError)
                            else "embedding_http_failure"
                            if status
                            else "embedding_transport_failure"
                            if transient
                            else "malformed_embedding_response",
                            category="timeout"
                            if isinstance(error, TimeoutError)
                            else "http"
                            if status
                            else "transport"
                            if transient
                            else "response",
                            stage="http" if transient or status else "validation",
                            http_status=status,
                            retryable=transient,
                            attempt=attempt,
                            cause_type=type(error).__name__,
                        )
                    )
                    ledger.failure(call_id, detail, time.monotonic() - started)
                await self.store.append(
                    incident,
                    "embedding_failure",
                    {
                        "request_id": request_id,
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                        "http_status": status,
                        "retryable": transient,
                    },
                    None,
                )
                if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt)):
                    raise
                if not transient:
                    raise DiagnosticError(
                        "Embedding request failed",
                        code="embedding_request_failed",
                        category="http" if status else "response",
                        stage="http" if status else "validation",
                        http_status=status,
                    ) from None
                if attempt > settings.max_retries:
                    raise EmbeddingUnavailable(
                        "Embedding retries exhausted",
                        code="embedding_unavailable",
                        category="transport",
                        stage="http",
                        retryable=True,
                        retry_exhausted=True,
                    ) from None
                delay = min(5, 0.5 * 2 ** (attempt - 1)) + random.uniform(0, 0.1)
                headers = getattr(getattr(error, "response", None), "headers", {})
                try:
                    delay = max(
                        delay,
                        float(headers.get("retry-after", 0)),
                        float(headers.get("retry-after-ms", 0)) / 1000,
                    )
                except (ValueError, TypeError):
                    pass
                if deadline and time.monotonic() + delay >= deadline:
                    raise TimeoutError(
                        "embedding retry exceeded incident deadline"
                    ) from None
                await asyncio.sleep(delay)


def validate_vector(vector, dimensions):
    if (
        not isinstance(vector, list)
        or len(vector) != dimensions
        or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)
        or not any(vector)
    ):
        raise ValueError("invalid embedding vector")
    return vector


def process_peak_rss():
    try:
        import resource
    except ImportError:
        return None
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if sys.platform == "darwin" else 1024
    )


class QdrantIndex:
    def __init__(self, dimensions):
        from qdrant_client import QdrantClient, models

        self.client, self.models = QdrantClient(":memory:"), models
        self.client.create_collection(
            "memory",
            vectors_config=models.VectorParams(
                size=dimensions, distance=models.Distance.COSINE
            ),
        )

    def upsert(self, key, vector):
        self.client.upsert("memory", [self.models.PointStruct(id=key, vector=vector)])

    def delete(self, keys):
        if keys:
            self.client.delete("memory", self.models.PointIdsList(points=keys))

    def search(self, vector, eligible, limit):
        if not eligible:
            return []
        hits = self.client.query_points(
            "memory",
            query=vector,
            query_filter=self.models.Filter(
                must=[self.models.HasIdCondition(has_id=sorted(eligible))]
            ),
            limit=limit,
        ).points
        return [(str(hit.id), hit.score) for hit in hits]

    def close(self):
        self.client.close()


class SemanticStore(SQLiteStore):
    """Index data is derived; all execution and history methods retain SQLite semantics."""

    def __init__(
        self, path, settings, base_dir=None, *, embedder=None, index_factory=QdrantIndex
    ):
        if settings.ledger is None:
            settings = settings.model_copy(
                update={"ledger": str(path.with_suffix(".embeddings-ledger.sqlite3"))}
            )
        self.settings = settings
        self.base = Path(base_dir or path.parent)
        super().__init__(
            path, self.base / settings.journal if settings.journal else None
        )
        self.index_factory = index_factory
        self.embedder = embedder or OpenAIEmbedder(settings, self, self.base)
        self.retrieval_lock = threading.Lock()
        self.incident = None
        self.index = None
        self.documents = {}
        self.edges = set()
        self.row_edges = {}
        self.indexed_hashes = {}
        self.watermark = 0
        self.hints = {}
        self.intents = {}
        self.requested_intents = {}
        self.last_result = None
        self.last_key = None
        try:
            with self.lock, self.connection:
                self.connection.executescript(
                    "CREATE TABLE IF NOT EXISTS semantic_embeddings(cache_key TEXT PRIMARY KEY,vector TEXT NOT NULL); CREATE TABLE IF NOT EXISTS semantic_pending(cache_key TEXT NOT NULL,incident_id TEXT NOT NULL,status TEXT NOT NULL,PRIMARY KEY(cache_key,incident_id));"
                )
        except BaseException:
            self.connection.close()
            raise

    async def set_memory_query_hints(self, state, hints):
        with self.lock:
            self.history_scope(state)
            self.hints[state.incident_id] = [
                MemoryQueryIntent.model_validate(h) for h in hints
            ]

    def _memory_incidents(self, incident, scope):
        # Legacy memory() is also used to form filtered history choices. Keep its
        # incident boundary equal to vector retrieval, even with equal scopes.
        return (
            [incident]
            if self.connection.execute(
                "SELECT 1 FROM memory_scopes WHERE incident_id=? AND scope=?",
                (incident, scope),
            ).fetchone()
            else []
        )

    async def memory_query_intents(self, state, candidates):
        with self.lock:
            self.history_scope(state)
            intents = query_intents(
                state,
                candidates,
                self.hints.get(state.incident_id, []),
                self.settings.max_queries,
            )
            # Keep only current intents; candidate generation and pre-execution validation
            # regenerate them against the same state and platform catalogue.
            self.intents[state.incident_id] = intents
        return {key: value.model_dump(mode="json") for key, value in intents.items()}

    def cache_key(self, text):
        return digest(
            [
                RENDERER_VERSION,
                self.settings.embedding_model,
                self.settings.dimensions,
                text,
            ]
        )

    async def vectors(self, texts, incident):
        unique = dict.fromkeys(texts)
        found, missing, blocked = {}, [], False
        with self.lock:
            for text in unique:
                key = self.cache_key(text)
                row = self.connection.execute(
                    "SELECT vector FROM semantic_embeddings WHERE cache_key=?", (key,)
                ).fetchone()
                if row:
                    found[text] = validate_vector(
                        json.loads(row[0]), self.settings.dimensions
                    )
                elif self.connection.execute(
                    "SELECT 1 FROM semantic_pending WHERE cache_key=? AND incident_id=?",
                    (key, incident),
                ).fetchone():
                    blocked = True
                else:
                    missing.append(text)
        # Per-request batches respect API input and configured token admission limits.
        batch, tokens = [], 0
        ceiling = min(
            300000,
            self.settings.embedding_tpm or 300000,
            self.settings.account_tpm or 300000,
        )
        batches = []
        for text in missing:
            size = estimate(text, self.settings.embedding_model)["estimated_tokens"]
            if size > ceiling:
                raise DiagnosticError(
                    "Embedding item exceeds account token allowance",
                    code="embedding_input_too_large",
                    category="configuration",
                    stage="preparation",
                )
            if batch and (tokens + size > ceiling or len(batch) >= 2048):
                batches.append(batch)
                batch, tokens = [], 0
            batch.append(text)
            tokens += size
        if batch:
            batches.append(batch)
        for batch in batches:
            # This commit precedes all remote work. Reopen must not reissue unknown jobs.
            with self.lock, self.connection:
                for text in batch:
                    self.connection.execute(
                        "INSERT INTO semantic_pending VALUES(?,?,'unknown')",
                        (self.cache_key(text), incident),
                    )
            try:
                vectors = await self.embedder.embed(batch, incident)
                if len(vectors) != len(batch):
                    raise ValueError("embedding count mismatch")
                validated = [
                    validate_vector(vector, self.settings.dimensions)
                    for vector in vectors
                ]
            except EmbeddingUnavailable:
                blocked = True
                break
            with self.lock, self.connection:
                for text, vector in zip(batch, validated, strict=True):
                    key = self.cache_key(text)
                    self.connection.execute(
                        "INSERT OR REPLACE INTO semantic_embeddings VALUES(?,?)",
                        (key, json.dumps(vector)),
                    )
                    self.connection.execute(
                        "DELETE FROM semantic_pending WHERE cache_key=? AND incident_id=?",
                        (key, incident),
                    )
                    found[text] = vector
        return found, blocked

    def _switch(self, incident):
        if self.incident != incident:
            if self.index:
                self.index.close()
            self.index = None
            self.incident = incident
            self.documents, self.edges = {}, set()
            self.row_edges, self.indexed_hashes = {}, {}
            self.watermark = 0
            self.last_key, self.last_result = None, None
            # Runtime resources remain bounded to the active incident.
            self.hints = {incident: self.hints.get(incident, [])}
            self.intents = {incident: self.intents.get(incident, {})}
            self.requested_intents = {incident: self.requested_intents.get(incident)}

    def recover_embeddings(self, incident):
        """Recover received responses after a cache-write interruption, without an API call."""
        with self.lock:
            if not self.connection.execute(
                "SELECT 1 FROM semantic_pending WHERE incident_id=?", (incident,)
            ).fetchone():
                return
            rows = self.connection.execute(
                "SELECT r.payload,e.payload FROM events r JOIN events e ON e.incident_id=r.incident_id AND json_extract(e.payload,'$.request_id')=json_extract(r.payload,'$.request_id') AND json_extract(e.payload,'$.attempt')=json_extract(r.payload,'$.attempt') WHERE r.incident_id=? AND r.kind='embedding_request' AND e.kind='embedding_response' ORDER BY e.sequence",
                (incident,),
            ).fetchall()
        for request_raw, response_raw in rows:
            request, received = json.loads(request_raw), json.loads(response_raw)
            value = received["response"]
            if (
                request["model"] != self.settings.embedding_model
                or request["dimensions"] != self.settings.dimensions
                or request["renderer"] != RENDERER_VERSION
            ):
                continue
            try:
                entries = value["data"]
                if value["model"] != self.settings.embedding_model or sorted(
                    e["index"] for e in entries
                ) != list(range(len(request["input"]))):
                    continue
                vectors = [
                    validate_vector(e["embedding"], self.settings.dimensions)
                    for e in sorted(entries, key=lambda e: e["index"])
                ]
                actual = value["usage"]["prompt_tokens"]
                if (
                    type(actual) is not int
                    or actual < 0
                    or value["usage"]["total_tokens"] != actual
                ):
                    continue
            except (ValueError, KeyError, TypeError):
                continue
            call_id = request.get("call_id")
            if call_id is not None:
                ledger = Ledger(Path(self.settings.ledger))
                with ledger.connect() as db:
                    call = db.execute(
                        "SELECT status FROM calls WHERE id=? AND trial=? AND endpoint='/v1/embeddings'",
                        (call_id, incident),
                    ).fetchone()
                if call and call[0] == "unknown":
                    ledger.reconcile(
                        call_id,
                        {"input_tokens": actual, "output_tokens": 0},
                        received.get("seconds", 0),
                    )
            with self.lock, self.connection:
                for text, vector in zip(request["input"], vectors, strict=True):
                    key = self.cache_key(text)
                    self.connection.execute(
                        "INSERT OR REPLACE INTO semantic_embeddings VALUES(?,?)",
                        (key, json.dumps(vector)),
                    )
                    self.connection.execute(
                        "DELETE FROM semantic_pending WHERE cache_key=? AND incident_id=?",
                        (key, incident),
                    )

    async def sync_memory(self, state):
        """Called while holding retrieval_lock; no network work on event append paths."""
        with self.lock:
            incident = self.history_scope(state)
            self._switch(incident)
            upper = self.connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM events WHERE incident_id=? AND kind IN ('incident_ingested','parse_outcome','execution_started','execution_result','execution_unknown','raw_output','hypothesis_assessment')",
                (incident,),
            ).fetchone()[0]
            # Action rows are updated in place: include executions touched after watermark.
            rows = self.connection.execute(
                "SELECT id,sequence,category,entity,tool,evidence_kind,payload FROM history_entries WHERE incident_id=? AND category IN ('fact','action') AND (sequence>? OR execution_id IN (SELECT json_extract(payload,'$.execution_id') FROM events WHERE incident_id=? AND sequence>? AND sequence<=?)) AND sequence<=? ORDER BY id",
                (incident, self.watermark, incident, self.watermark, upper, upper),
            ).fetchall()
            hypotheses = self.connection.execute(
                "SELECT entity,sequence,payload FROM memory_hypotheses WHERE incident_id=? AND sequence>? AND sequence<=?",
                (incident, self.watermark, upper),
            ).fetchall()
        for row_id, sequence, category, entity, tool, kind, raw in rows:
            if tool in {"history.query", "memory.search"}:
                continue
            payload = json.loads(raw)
            if category == "fact" and kind in {"history", "memory_retrieval"}:
                continue
            prior = [key for key, doc in self.documents.items() if doc.row_id == row_id]
            if self.index:
                self.index.delete(prior)
            for key in prior:
                del self.documents[key]
                self.indexed_hashes.pop(key, None)
            self.row_edges[row_id] = {
                (source, target, relation, f"{incident}:{sequence}")
                for source, target, relation in relationships(payload)
            }
            outcome = payload.get("outcome", {})
            content = (
                payload.get("payload", {})
                if category == "fact"
                else {
                    "tool": tool,
                    "args": payload["candidate"]["args"],
                    "status": payload.get("status"),
                    "detail": outcome.get("detail"),
                    "parse_status": outcome.get("parse", {}).get("status"),
                    "unknown_reason": payload.get("unknown_reason"),
                }
            )
            reject_credentials(content)
            parts = document_texts(
                content,
                entity,
                tool,
                kind,
                self.settings.embedding_model,
                self.settings.chunk_tokens,
            )
            for segment, text in enumerate(parts):
                doc_id = str(uuid5(NAMESPACE_URL, f"{incident}:{row_id}:{segment}"))
                self.documents[doc_id] = Document(
                    doc_id,
                    row_id,
                    category,
                    entity,
                    tool,
                    kind,
                    text,
                    self.cache_key(text),
                    segment,
                    len(parts),
                    sequence,
                )
        for entity, sequence, raw in hypotheses:
            prior = [
                key
                for key, doc in self.documents.items()
                if doc.category == "hypothesis" and doc.entity == entity
            ]
            if self.index:
                self.index.delete(prior)
            for key in prior:
                del self.documents[key]
                self.indexed_hashes.pop(key, None)
            parts = document_texts(
                {"interpretation": True, "hypothesis": json.loads(raw)},
                entity,
                "",
                "interpretation",
                self.settings.embedding_model,
                self.settings.chunk_tokens,
            )
            for segment, text in enumerate(parts):
                doc_id = str(
                    uuid5(NAMESPACE_URL, f"{incident}:hypothesis:{entity}:{segment}")
                )
                self.documents[doc_id] = Document(
                    doc_id,
                    -sequence,
                    "hypothesis",
                    entity,
                    "",
                    "interpretation",
                    text,
                    self.cache_key(text),
                    segment,
                    len(parts),
                    sequence,
                )
        self.edges = set().union(*self.row_edges.values()) if self.row_edges else set()
        self.watermark = upper
        if self.settings.mode == "deterministic":
            return False
        self.recover_embeddings(incident)
        vectors, degraded = await self.vectors(
            [d.text for d in self.documents.values()], incident
        )
        if self.index is None and vectors:
            self.index = self.index_factory(self.settings.dimensions)
        if self.index:
            for document in self.documents.values():
                if (
                    document.text in vectors
                    and self.indexed_hashes.get(document.id) != document.content_hash
                ):
                    self.index.upsert(document.id, vectors[document.text])
                    self.indexed_hashes[document.id] = document.content_hash
        return degraded

    def hydrate(self, state, document, reasons):
        with self.lock:
            self.history_scope(state)
            row = (
                self.connection.execute(
                    "SELECT sequence,payload FROM memory_hypotheses WHERE incident_id=? AND entity=?",
                    (state.incident_id, document.entity),
                ).fetchone()
                if document.category == "hypothesis"
                else self.connection.execute(
                    "SELECT sequence,payload FROM history_entries WHERE id=? AND incident_id=?",
                    (document.row_id, state.incident_id),
                ).fetchone()
            )
            if row is None:
                raise ValueError("retrieval returned unowned evidence")
            sequence, raw = row
            value = json.loads(raw)
            source = self.connection.execute(
                "SELECT payload FROM events WHERE incident_id=? AND sequence=?",
                (state.incident_id, sequence),
            ).fetchone()
            parser_version = (
                json.loads(source[0]).get("parse", {}).get("parser_version")
                if source
                else None
            )
            refs = value.get("raw_output_refs", [])
            if len(self.saved_result(state.incident_id, refs)) != len(refs):
                raise ValueError("retrieval returned unowned artifact")
            if document.category == "fact":
                versions = self.connection.execute(
                    "SELECT payload FROM memory_facts WHERE incident_id=? AND fact_key=? AND resource_id=?",
                    (state.incident_id, value["key"], value["resource_id"]),
                ).fetchall()
                if len({digest(json.loads(version[0])) for version in versions}) > 1:
                    value["contradiction"] = True
                    if value["status"] == "observed":
                        value["status"] = "contradicted"
        if document.category == "fact":
            pinned = bool(
                value.get("immutable_snapshot")
                and value["immutable_snapshot"]
                == state.payload.get("index_fingerprint")
            )
            value.update(
                freshness="pinned_snapshot" if pinned else "live_original_timestamp",
                evidence_version=value.get("immutable_snapshot"),
                historical=False,
            )
            value["payload"] = {
                "retrieved_text": document.text,
                "segment": document.segment,
                "segments": document.segments,
                "incomplete_segment": document.segments > 1,
                "entity": document.entity,
                "kind": document.evidence_kind,
            }
        elif document.category == "hypothesis":
            value = {
                "entity": document.entity,
                "interpretation": True,
                "retrieved_text": document.text,
                "segment": document.segment,
                "segments": document.segments,
                "incomplete_segment": document.segments > 1,
            }
        else:
            # Output is represented by bounded indexed text; full originals stay accessible.
            value = {
                "identity": value.get("identity"),
                "tool": document.tool,
                "args": value["candidate"]["args"],
                "status": value.get("status"),
                "started_at": value.get("started_at"),
                "finished_at": value.get("finished_at"),
                "raw_output_refs": refs,
                "retrieved_text": document.text,
                "segment": document.segment,
                "segments": document.segments,
                "incomplete_segment": document.segments > 1,
            }
        value.update(
            document_id=document.id,
            event_ref=f"{state.incident_id}:{sequence}",
            retrieval_reasons=reasons,
            parser_version=parser_version,
        )
        return value

    def baseline(self, state):
        with self.lock:
            self.history_scope(state)
            outcomes = dict(
                self.connection.execute(
                    "SELECT status,COUNT(*) FROM memory_actions WHERE incident_id=? GROUP BY status",
                    (state.incident_id,),
                )
            )
            count = self.connection.execute(
                "SELECT COUNT(*) FROM memory_facts WHERE incident_id=?",
                (state.incident_id,),
            ).fetchone()[0]
        return {
            "facts": [],
            "actions": [],
            "hypotheses": [],
            "progress": {
                "actions": sum(outcomes.values()),
                "facts": count,
                "outcomes": outcomes,
            },
            "historical": False,
        }

    async def retrieve_memory(self, state, candidates, intent=None):
        while not self.retrieval_lock.acquire(blocking=False):
            await asyncio.sleep(0.01)
        started = time.monotonic()
        try:
            degraded = await self.sync_memory(state)
            if intent is not None:
                values = self.intents.get(state.incident_id, {})
                if intent not in values:
                    raise ValueError("unknown or expired recall intent")
                queries = {intent: values[intent]}
            else:
                queries = query_intents(
                    state,
                    candidates,
                    self.hints.get(state.incident_id, []),
                    self.settings.max_queries,
                )
                requested = self.requested_intents.pop(state.incident_id, None)
                if requested is not None:
                    queries = {digest(requested.model_dump()): requested, **queries}
                    queries = dict(list(queries.items())[: self.settings.max_queries])
            qvectors = {}
            if self.settings.mode != "deterministic" and self.documents:
                qvectors, unavailable = await self.vectors(
                    [q.text for q in queries.values()], state.incident_id
                )
                degraded |= unavailable
            key = digest(
                [
                    state.incident_id,
                    self.watermark,
                    [d.content_hash for d in self.documents.values()],
                    {k: v.model_dump() for k, v in queries.items()},
                    sorted(qvectors),
                    degraded,
                ]
            )
            if self.last_key == key:
                result = copy.deepcopy(self.last_result)
                result["retrieval"]["cache_hit"] = True
                result["retrieval"]["seconds"] = time.monotonic() - started
                return result

            def search(query_id, eligible):
                vector = qvectors.get(queries[query_id].text)
                return (
                    self.index.search(vector, eligible, self.settings.per_query)
                    if self.index and vector
                    else []
                )

            ordered, reasons = rank(
                self.documents,
                queries,
                search,
                self.edges,
                self.settings.mode == "graph",
                self.settings.per_query,
            )
            if self.settings.mode == "deterministic" and intent is None:
                ordered = sorted(
                    self.documents,
                    key=lambda key: (
                        -self.documents[key].sequence,
                        self.documents[key].segment,
                    ),
                )
                reasons = {key: [{"method": "recent"}] for key in ordered}
            memory = self.baseline(state)
            while (
                memory["hypotheses"]
                and estimate(memory)["estimated_tokens"] > self.settings.memory_tokens
            ):
                memory["hypotheses"].pop()
            used = estimate(memory)["estimated_tokens"]
            if used > self.settings.memory_tokens:
                raise ValueError("essential cumulative memory exceeds retrieval budget")
            selected, omitted, seen = [], [], set()
            for doc_id in ordered:
                document = self.documents[doc_id]
                # Identical content can share a vector, but evidence occurrences retain provenance.
                signature = (document.category, document.entity, document.content_hash)
                if signature in seen:
                    continue
                seen.add(signature)
                item = self.hydrate(state, document, reasons[doc_id])
                size = estimate(item)["estimated_tokens"]
                if (
                    len(selected) >= self.settings.max_documents
                    or used + size > self.settings.memory_tokens
                ):
                    omitted.append(
                        {
                            "document_id": doc_id,
                            "event_ref": item["event_ref"],
                            "segment": document.segment,
                        }
                    )
                    continue
                section = {
                    "fact": "facts",
                    "action": "actions",
                    "hypothesis": "hypotheses",
                }[document.category]
                memory[section].append(item)
                selected.append(doc_id)
                used += size
            # Publish evidence not considered by top-k retrieval as well as budget omissions.
            omitted.extend(
                {
                    "document_id": d.id,
                    "event_ref": self._document_ref(d, state.incident_id),
                    "segment": d.segment,
                }
                for d in self.documents.values()
                if d.id not in ordered
            )
            memory["progress"].update(
                omitted_facts=len(omitted), indexed_documents=len(self.documents)
            )
            retrieval = {
                "version": RENDERER_VERSION,
                "mode": self.settings.mode,
                "status": "degraded" if degraded else "ok",
                "index_watermark": self.watermark,
                "indexed_documents": len(self.documents),
                "vector_documents": self._vector_count(),
                "estimated_vector_bytes": len(self.indexed_hashes)
                * self.settings.dimensions
                * 4,
                "process_peak_rss_bytes": process_peak_rss(),
                "queries": {k: v.model_dump(mode="json") for k, v in queries.items()},
                "selected": selected,
                "omitted_count": len(omitted),
                "omitted": omitted[:24],
                "omitted_reference": {
                    "tool": "history.query",
                    "query": {"category": "fact"},
                },
                "budget_tokens": self.settings.memory_tokens,
                "estimated_memory_tokens": used,
                "cache_hit": False,
                "seconds": time.monotonic() - started,
                "original_live_evidence_requires_revalidation": True,
            }
            result = {"memory": memory, "retrieval": retrieval}
            await self.append(state.incident_id, "memory_retrieval", retrieval, None)
            self.last_key, self.last_result = key, copy.deepcopy(result)
            return result
        finally:
            self.retrieval_lock.release()

    def _vector_count(self):
        return len(self.indexed_hashes)

    def _document_ref(self, document, incident):
        if document.category == "hypothesis":
            return f"{incident}:{-document.row_id}"
        with self.lock:
            row = self.connection.execute(
                "SELECT sequence FROM history_entries WHERE id=? AND incident_id=?",
                (document.row_id, incident),
            ).fetchone()
        if row is None:
            raise ValueError("unowned document")
        return f"{incident}:{row[0]}"

    async def search_memory(self, state, intent):
        result = await self.retrieve_memory(state, [], intent)
        self.requested_intents[state.incident_id] = self.intents[state.incident_id][
            intent
        ]
        return result

    async def aclose(self):
        if self.index:
            self.index.close()
        self.documents.clear()
        self.edges.clear()
        await super().aclose()


@factory(
    subsystem="event_store",
    component_type=SemanticStore,
    settings_model=SemanticSettings,
)
def semantic_store(settings, context):
    if settings.ledger:
        settings = settings.model_copy(
            update={"ledger": str(context.base_dir / settings.ledger)}
        )
    return SemanticStore(context.base_dir / settings.path, settings, context.base_dir)

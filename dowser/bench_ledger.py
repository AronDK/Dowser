"""Durable, transactional campaign spending reservations for every HTTP attempt."""

import json
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

from .bench_data import confined, file_hash
from .diagnostics import failure_details

PRICE_NANODOLLARS_PER_TOKEN = 42  # $0.042 / million input tokens
REQUEST_CEILING = 64000
_INHERIT = object()


class SpendingLimit(RuntimeError):
    pass


class CallLimit(RuntimeError):
    pass


class Ledger:
    def __init__(
        self,
        path,
        *,
        budget=_INHERIT,
        price=PRICE_NANODOLLARS_PER_TOKEN,
        call_limit=_INHERIT,
    ):
        self.path = Path(path)
        confined(self.path.parent, self.path.name, must_exist=False)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(
                "CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY, trial TEXT NOT NULL, status TEXT NOT NULL, reserved INTEGER NOT NULL, input_tokens INTEGER, output_tokens INTEGER, latency REAL, error TEXT); CREATE TABLE IF NOT EXISTS policy(budget INTEGER,price INTEGER,call_limit INTEGER);"
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(calls)")}
            for name, definition in {
                "attempt": "INTEGER NOT NULL DEFAULT 1",
                "request_id": "TEXT",
                "failure_details": "TEXT",
                "timing_seconds": "TEXT",
            }.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE calls ADD COLUMN {name} {definition}")
            db.execute(
                "CREATE TABLE IF NOT EXISTS prior_spending(source TEXT PRIMARY KEY,sha256 TEXT NOT NULL,accounted INTEGER NOT NULL,calls INTEGER NOT NULL,unknown_calls INTEGER NOT NULL)"
            )
            policy = db.execute("SELECT * FROM policy").fetchone()
            if budget is _INHERIT:
                budget = policy[0] if policy else None
            if call_limit is _INHERIT:
                call_limit = policy[2] if policy else None
            self.budget, self.price, self.call_limit = budget, price, call_limit
            if policy and policy != (budget, price, call_limit):
                raise ValueError("spending policy changed on resume")
            if not policy:
                db.execute(
                    "INSERT INTO policy VALUES(?,?,?)", (budget, price, call_limit)
                )

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def prior_snapshot(path):
        path = confined(Path(path).parent, Path(path).name)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            calls, accounted, unknown = db.execute(
                "SELECT COUNT(*),COALESCE(SUM(reserved),0),COALESCE(SUM(status='unknown'),0) FROM calls"
            ).fetchone()
            if db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prior_spending'"
            ).fetchone():
                extra = db.execute(
                    "SELECT COALESCE(SUM(calls),0),COALESCE(SUM(accounted),0),COALESCE(SUM(unknown_calls),0) FROM prior_spending"
                ).fetchone()
                calls, accounted, unknown = (
                    calls + extra[0],
                    accounted + extra[1],
                    unknown + extra[2],
                )
        return {
            "path": str(path),
            "sha256": file_hash(path),
            "accounted_nanodollars": accounted,
            "calls": calls,
            "unknown_calls": unknown,
        }

    def inherit(self, snapshot):
        if any(
            type(snapshot[k]) is not int or snapshot[k] < 0
            for k in ("accounted_nanodollars", "calls", "unknown_calls")
        ):
            raise ValueError("invalid previous spending")
        if Path(snapshot["path"]) == self.path.absolute():
            raise ValueError("ledger cannot inherit itself")
        if self.prior_snapshot(Path(snapshot["path"])) != snapshot:
            raise ValueError("previous spending snapshot changed")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            value = (
                snapshot["path"],
                snapshot["sha256"],
                snapshot["accounted_nanodollars"],
                snapshot["calls"],
                snapshot["unknown_calls"],
            )
            existing = db.execute(
                "SELECT * FROM prior_spending WHERE source=?", (value[0],)
            ).fetchone()
            if existing and existing != value:
                raise ValueError("previous spending changed")
            if not existing:
                if self.budget is not None and (
                    snapshot["accounted_nanodollars"]
                    + self.stats()["accounted_nanodollars"]
                    > self.budget
                ):
                    raise SpendingLimit("previous spending exceeds campaign ceiling")
                db.execute("INSERT INTO prior_spending VALUES(?,?,?,?,?)", value)

    def reserve(self, trial, *, attempt=1, request_id=None):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if self.call_limit is not None and (
                db.execute(
                    "SELECT COUNT(*) FROM calls WHERE trial=?", (trial,)
                ).fetchone()[0]
                >= self.call_limit
            ):
                raise CallLimit(f"{self.call_limit}-call trial limit reached")
            spent = db.execute(
                "SELECT COALESCE(SUM(reserved),0) FROM calls"
            ).fetchone()[0]
            spent += db.execute(
                "SELECT COALESCE(SUM(accounted),0) FROM prior_spending"
            ).fetchone()[0]
            amount = REQUEST_CEILING * self.price
            if self.budget is not None and spent + amount > self.budget:
                raise SpendingLimit("campaign spending ceiling reached")
            return db.execute(
                "INSERT INTO calls(trial,status,reserved,attempt,request_id) VALUES(?,'unknown',?,?,?)",
                (trial, amount, attempt, request_id),
            ).lastrowid

    def reconcile(self, call_id, usage, latency):
        if (
            any(
                type(usage.get(k)) is not int or usage[k] < 0
                for k in ("input_tokens", "output_tokens")
            )
            or usage["input_tokens"] > REQUEST_CEILING
        ):
            raise ValueError("usage outside documented ceiling; reservation retained")
        with self.connect() as db:
            updated = db.execute(
                "UPDATE calls SET status='known',reserved=?,input_tokens=?,output_tokens=?,latency=? WHERE id=? AND status='unknown'",
                (
                    usage["input_tokens"] * self.price,
                    usage["input_tokens"],
                    usage["output_tokens"],
                    latency,
                    call_id,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("reservation missing or already reconciled")

    def failure(self, call_id, error, latency=None):
        with self.connect() as db:
            db.execute(
                "UPDATE calls SET error=?,latency=COALESCE(?,latency),failure_details=? WHERE id=?",
                (
                    type(error).__name__,
                    latency,
                    json.dumps(failure_details(error)),
                    call_id,
                ),
            )

    def timing(self, call_id, spans):
        with self.connect() as db:
            db.execute(
                "UPDATE calls SET timing_seconds=? WHERE id=?",
                (json.dumps(spans), call_id),
            )

    def timings(self, trial):
        with self.connect() as db:
            return [
                json.loads(row[0])
                for row in db.execute(
                    "SELECT timing_seconds FROM calls WHERE trial=? AND timing_seconds IS NOT NULL ORDER BY id",
                    (trial,),
                )
            ]

    def stats(self, trial=None):
        with self.connect() as db:
            sql = "SELECT COUNT(*),COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(reserved),0),COALESCE(SUM(latency),0),COALESCE(SUM(status='unknown'),0) FROM calls"
            row = db.execute(
                sql + (" WHERE trial=?" if trial else ""), (trial,) if trial else ()
            ).fetchone()
            prior = (
                db.execute(
                    "SELECT COALESCE(SUM(accounted),0),COALESCE(SUM(calls),0),COALESCE(SUM(unknown_calls),0) FROM prior_spending"
                ).fetchone()
                if not trial
                else (0, 0, 0)
            )
            retries = db.execute(
                "SELECT COUNT(*) FROM calls WHERE attempt>1"
                + (" AND trial=?" if trial else ""),
                (trial,) if trial else (),
            ).fetchone()[0]
        result = dict(
            zip(
                (
                    "calls",
                    "input_tokens",
                    "output_tokens",
                    "accounted_nanodollars",
                    "model_latency_seconds",
                    "unknown_calls",
                ),
                row,
                strict=True,
            )
        )

        result["accounted_nanodollars"] += prior[0]
        result.update(
            prior_accounted_nanodollars=prior[0],
            prior_calls=prior[1],
            prior_unknown_calls=prior[2],
            retry_calls=retries,
            budget_nanodollars=self.budget,
        )
        return result

    def diagnostics(self, trial=None):
        with self.connect() as db:
            rows = db.execute(
                "SELECT id,trial,attempt,request_id,status,failure_details FROM calls WHERE failure_details IS NOT NULL"
                + (" AND trial=?" if trial else "")
                + " ORDER BY id",
                (trial,) if trial else (),
            ).fetchall()
        return [
            {
                "call_id": r[0],
                "trial": r[1],
                "attempt": r[2],
                "request_id": r[3],
                "usage_status": r[4],
                "failure": json.loads(r[5]),
            }
            for r in rows
        ]

    def last_error(self, trial):
        with self.connect() as db:
            row = db.execute(
                "SELECT error FROM calls WHERE trial=? AND error IS NOT NULL ORDER BY id DESC LIMIT 1",
                (trial,),
            ).fetchone()
        return row[0] if row else None

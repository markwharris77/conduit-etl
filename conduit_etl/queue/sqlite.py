"""SQLiteQueue — durable queue backed by a local SQLite file.

Survives scheduler restarts. Uses WAL mode + ``BEGIN IMMEDIATE`` transactions
for safe concurrent access from a single process. Not intended for multiple
concurrent scheduler instances — use PostgresQueue for HA.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from conduit_etl.core.errors import QueueError
from conduit_etl.core.models import Job, Snapshot
from conduit_etl.queue.base import QueueBackend


_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    step_name TEXT NOT NULL,
    level INTEGER NOT NULL,
    input_snapshots TEXT NOT NULL,   -- JSON
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | claimed | done | failed
    claimed_by TEXT,
    started_at TEXT,
    last_heartbeat TEXT,
    output_snapshot TEXT,            -- JSON, set on done
    error TEXT
);
CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status);
"""


def _now() -> str:
    return datetime.utcnow().isoformat()


def _parse_dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"],
        step_name=row["step_name"],
        level=row["level"],
        input_snapshots=json.loads(row["input_snapshots"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        claimed_by=row["claimed_by"],
        started_at=_parse_dt(row["started_at"]),
    )


class SQLiteQueue(QueueBackend):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._path_str = str(self.path)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self._path_str, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(_DDL)
        conn.commit()

    def enqueue(self, job: Job) -> None:
        conn = self._conn()
        conn.execute(
            "INSERT OR IGNORE INTO jobs "
            "(id, step_name, level, input_snapshots, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, 'pending')",
            [job.id, job.step_name, job.level, json.dumps(job.input_snapshots),
             job.created_at.isoformat()],
        )
        conn.commit()

    def claim(self, worker_id: str) -> Job | None:
        conn = self._conn()
        # BEGIN IMMEDIATE prevents another thread from claiming the same row.
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM jobs WHERE status = 'pending' ORDER BY level, created_at LIMIT 1"
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return None
            now = _now()
            conn.execute(
                "UPDATE jobs SET status='claimed', claimed_by=?, started_at=?, last_heartbeat=? "
                "WHERE id=?",
                [worker_id, now, now, row["id"]],
            )
            conn.execute("COMMIT")
            job = _row_to_job(row)
            job.claimed_by = worker_id
            job.started_at = datetime.fromisoformat(now)
            return job
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def heartbeat(self, job_id: str, worker_id: str) -> None:
        conn = self._conn()
        n = conn.execute(
            "UPDATE jobs SET last_heartbeat=? WHERE id=? AND claimed_by=? AND status='claimed'",
            [_now(), job_id, worker_id],
        ).rowcount
        conn.commit()
        if n == 0:
            raise QueueError(f"heartbeat failed for job {job_id!r} / worker {worker_id!r}")

    def complete(self, job_id: str, output_snapshot: Snapshot) -> None:
        snap_json = json.dumps({
            "id": output_snapshot.id,
            "table": output_snapshot.table,
            "rows": output_snapshot.rows,
        })
        conn = self._conn()
        conn.execute(
            "UPDATE jobs SET status='done', output_snapshot=? WHERE id=?",
            [snap_json, job_id],
        )
        conn.commit()

    def fail(self, job_id: str, error: str) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE jobs SET status='failed', error=? WHERE id=?",
            [error, job_id],
        )
        conn.commit()

    def requeue_stale(self, heartbeat_window_seconds: int) -> int:
        cutoff = (datetime.utcnow() - timedelta(seconds=heartbeat_window_seconds)).isoformat()
        conn = self._conn()
        n = conn.execute(
            "UPDATE jobs SET status='pending', claimed_by=NULL, started_at=NULL, last_heartbeat=NULL "
            "WHERE status='claimed' AND last_heartbeat < ?",
            [cutoff],
        ).rowcount
        conn.commit()
        return n

    def pending_count(self) -> int:
        conn = self._conn()
        row = conn.execute("SELECT count(*) FROM jobs WHERE status='pending'").fetchone()
        return row[0] if row else 0

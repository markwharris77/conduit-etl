"""PostgresQueue — durable queue using PostgreSQL FOR UPDATE SKIP LOCKED.

Supports multiple concurrent scheduler instances safely — each claim is
an atomic ``SELECT ... FOR UPDATE SKIP LOCKED`` so two schedulers never
claim the same job. Requires ``psycopg[binary]>=3.1``.

Config example (pipeline.toml):

    [queue]
    backend = "postgres"
    url     = "${DATABASE_URL}"
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

try:
    import psycopg  # type: ignore[import]
    _PSYCOPG_AVAILABLE = True
except ImportError:
    _PSYCOPG_AVAILABLE = False

from conduit_etl.core.errors import QueueError
from conduit_etl.core.models import Job, Snapshot
from conduit_etl.queue.base import QueueBackend

_DDL = """
CREATE TABLE IF NOT EXISTS conduit_jobs (
    id TEXT PRIMARY KEY,
    step_name TEXT NOT NULL,
    level INTEGER NOT NULL DEFAULT 0,
    input_snapshots JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_by TEXT,
    started_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    output_snapshot JSONB,
    error TEXT
);
CREATE INDEX IF NOT EXISTS conduit_jobs_status ON conduit_jobs (status);
CREATE INDEX IF NOT EXISTS conduit_jobs_level_created ON conduit_jobs (level, created_at)
    WHERE status = 'pending';
"""


def _require_psycopg() -> None:
    if not _PSYCOPG_AVAILABLE:
        raise ImportError(
            "psycopg is required for PostgresQueue. "
            "Install it with: pip install conduit-etl[postgres]"
        )


def _row_to_job(row: tuple) -> Job:
    id_, step_name, level, input_snapshots, created_at, claimed_by, started_at = row
    return Job(
        id=id_,
        step_name=step_name,
        level=level,
        input_snapshots=input_snapshots if isinstance(input_snapshots, dict) else json.loads(input_snapshots),
        created_at=created_at,
        claimed_by=claimed_by,
        started_at=started_at,
    )


class PostgresQueue(QueueBackend):
    def __init__(self, url: str) -> None:
        _require_psycopg()
        self._url = url
        self._init_db()

    def _connect(self):
        return psycopg.connect(self._url, autocommit=False)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(_DDL)
            conn.commit()

    def enqueue(self, job: Job) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO conduit_jobs (id, step_name, level, input_snapshots, created_at) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                [job.id, job.step_name, job.level, json.dumps(job.input_snapshots),
                 job.created_at],
            )
            conn.commit()

    def claim(self, worker_id: str) -> Job | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, step_name, level, input_snapshots, created_at, claimed_by, started_at "
                "FROM conduit_jobs WHERE status = 'pending' "
                "ORDER BY level, created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
            ).fetchone()
            if row is None:
                return None
            now = datetime.now()
            conn.execute(
                "UPDATE conduit_jobs SET status='claimed', claimed_by=%s, "
                "started_at=%s, last_heartbeat=%s WHERE id=%s",
                [worker_id, now, now, row[0]],
            )
            conn.commit()
            job = _row_to_job(row)
            job.claimed_by = worker_id
            job.started_at = now
            return job

    def heartbeat(self, job_id: str, worker_id: str) -> None:
        with self._connect() as conn:
            n = conn.execute(
                "UPDATE conduit_jobs SET last_heartbeat=%s "
                "WHERE id=%s AND claimed_by=%s AND status='claimed'",
                [datetime.now(), job_id, worker_id],
            ).rowcount
            conn.commit()
        if n == 0:
            raise QueueError(f"heartbeat failed for job {job_id!r} / worker {worker_id!r}")

    def complete(self, job_id: str, output_snapshot: Snapshot) -> None:
        snap_json = json.dumps({"id": output_snapshot.id, "table": output_snapshot.table,
                                "rows": output_snapshot.rows})
        with self._connect() as conn:
            conn.execute(
                "UPDATE conduit_jobs SET status='done', output_snapshot=%s WHERE id=%s",
                [snap_json, job_id],
            )
            conn.commit()

    def fail(self, job_id: str, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE conduit_jobs SET status='failed', error=%s WHERE id=%s",
                [error, job_id],
            )
            conn.commit()

    def requeue_stale(self, heartbeat_window_seconds: int) -> int:
        cutoff = datetime.now() - timedelta(seconds=heartbeat_window_seconds)
        with self._connect() as conn:
            n = conn.execute(
                "UPDATE conduit_jobs SET status='pending', claimed_by=NULL, "
                "started_at=NULL, last_heartbeat=NULL "
                "WHERE status='claimed' AND last_heartbeat < %s",
                [cutoff],
            ).rowcount
            conn.commit()
        return n

    def pending_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT count(*) FROM conduit_jobs WHERE status='pending'"
            ).fetchone()
            return row[0] if row else 0

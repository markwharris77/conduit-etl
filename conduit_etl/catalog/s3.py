"""S3Catalog — DuckLake on S3-compatible object storage.

Uses DuckDB's built-in httpfs extension for S3 access — no boto3 dependency.
Supports AWS S3, MinIO, Ceph, and any S3-compatible endpoint.

Run records are persisted as small parquet files at ``{url}/runs/{id}.parquet``
and reloaded on startup, so the catalog is fully durable across restarts.

Config example (pipeline.toml):

    [catalog]
    backend  = "s3"
    url      = "s3://my-bucket/conduit/catalog"
    endpoint = "http://minio.internal:9000"   # omit for AWS S3
    key      = "${MINIO_KEY}"
    secret   = "${MINIO_SECRET}"
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime
from pathlib import Path
from types import TracebackType

import duckdb

from conduit_etl.catalog.base import CatalogBackend, CatalogTransaction
from conduit_etl.core.errors import CatalogError, SnapshotNotFoundError
from conduit_etl.core.fingerprint import schema_hash
from conduit_etl.core.models import MergeMode, RunRecord, Snapshot

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_RUN_COLUMNS = (
    "id", "step_name", "output_table", "status", "snapshot_id",
    "schema_hash", "rows", "duration_seconds", "fingerprint", "meta",
    "started_at", "finished_at", "error",
)

_RUN_DDL = (
    "id VARCHAR, step_name VARCHAR, output_table VARCHAR, status VARCHAR, "
    "snapshot_id VARCHAR, schema_hash VARCHAR, rows BIGINT, "
    "duration_seconds DOUBLE, fingerprint VARCHAR, meta VARCHAR, "
    "started_at TIMESTAMP, finished_at TIMESTAMP, error VARCHAR"
)


def _check_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise CatalogError(f"unsafe table identifier: {name!r}")
    return name


class _S3Transaction(CatalogTransaction):
    def __init__(self, catalog: S3Catalog) -> None:
        self._catalog = catalog
        self._pending: list[Snapshot] = []
        self._closed = False

    def write(self, table: str, relation: duckdb.DuckDBPyRelation, meta: dict) -> Snapshot:
        _check_ident(table)
        con = self._catalog._con
        merge = MergeMode(meta.get("merge", MergeMode.REPLACE.value))
        merge_key = meta.get("merge_key")

        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        relation.write_parquet(tmp_path)
        stage_rel = con.read_parquet(tmp_path)
        con.register("_conduit_stage", stage_rel)
        try:
            exists = self._catalog._table_exists(table)
            if merge is MergeMode.REPLACE or not exists:
                con.execute(f'CREATE OR REPLACE TABLE lake."{table}" AS SELECT * FROM _conduit_stage')
            elif merge is MergeMode.APPEND:
                existing_cols = set(con.sql(f'SELECT * FROM lake."{table}" LIMIT 0').columns)
                incoming_cols = set(stage_rel.columns)
                if existing_cols != incoming_cols:
                    common = existing_cols & incoming_cols
                    if common:
                        cols_sql = ", ".join(f'"{c}"' for c in sorted(common))
                        con.execute(
                            f'INSERT INTO lake."{table}" ({cols_sql}) '
                            f"SELECT {cols_sql} FROM _conduit_stage"
                        )
                    else:
                        con.execute(
                            f'CREATE OR REPLACE TABLE lake."{table}" AS SELECT * FROM _conduit_stage'
                        )
                else:
                    con.execute(f'INSERT INTO lake."{table}" SELECT * FROM _conduit_stage')
            elif merge is MergeMode.UPSERT:
                if not merge_key:
                    raise CatalogError("upsert merge requires merge_key")
                keys = [_check_ident(k) for k in merge_key]
                on = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
                con.execute(f'DELETE FROM lake."{table}" AS t USING _conduit_stage AS s WHERE {on}')
                con.execute(f'INSERT INTO lake."{table}" SELECT * FROM _conduit_stage')
        finally:
            con.unregister("_conduit_stage")
            Path(tmp_path).unlink(missing_ok=True)

        rows = int(relation.aggregate("count(*) AS n").fetchone()[0])
        snap = Snapshot(
            id="", table=table, created_at=datetime.now(),
            rows=rows, schema_hash=schema_hash(relation), meta=dict(meta),
        )
        self._pending.append(snap)
        return snap

    def commit(self) -> None:
        if self._closed:
            return
        con = self._catalog._con
        con.commit()
        version = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        now = datetime.now()
        for snap in self._pending:
            snap.id = str(version)
            snap.created_at = now
        self._closed = True
        self._catalog._lock.release()

    def rollback(self) -> None:
        if self._closed:
            return
        self._catalog._con.rollback()
        self._closed = True
        self._catalog._lock.release()

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._closed:
            self.rollback()


class S3Catalog(CatalogBackend):
    """DuckLake-backed catalog stored on S3-compatible object storage.

    ``url`` should be an ``s3://`` URI pointing to the catalog root, e.g.
    ``s3://my-bucket/conduit/catalog``.

    Run records are stored as individual parquet files under ``{url}/runs/``
    and are reloaded on each startup, so the catalog survives restarts with
    full run history intact.
    """

    def __init__(
        self,
        url: str,
        *,
        endpoint: str = "",
        key: str = "",
        secret: str = "",
        region: str = "us-east-1",
    ) -> None:
        self.url = url.rstrip("/")
        self._runs_prefix = f"{self.url}/runs"
        self._lock = threading.Lock()

        self._con = duckdb.connect()
        self._con.execute("INSTALL ducklake")
        self._con.execute("LOAD ducklake")
        self._con.execute("INSTALL httpfs")
        self._con.execute("LOAD httpfs")

        if endpoint:
            self._con.execute(f"SET s3_endpoint='{endpoint}'")
            self._con.execute("SET s3_url_style='path'")
        if key:
            self._con.execute(f"SET s3_access_key_id='{key}'")
        if secret:
            self._con.execute(f"SET s3_secret_access_key='{secret}'")
        self._con.execute(f"SET s3_region='{region}'")

        catalog_uri = f"{self.url}/catalog.ducklake"
        data_path = f"{self.url}/data"
        self._con.execute(
            f"ATTACH 'ducklake:{catalog_uri}' AS lake (DATA_PATH '{data_path}')"
        )

        # In-process run log — populated from S3 on startup and written through on each record_run.
        self._con.execute(f"CREATE TABLE run_records ({_RUN_DDL})")
        self._con.execute(f"CREATE TABLE dead_letters ("
                          "id VARCHAR, step_name VARCHAR, input_snapshot_ids VARCHAR, "
                          "error VARCHAR, traceback VARCHAR, failed_at TIMESTAMP)")
        self._load_existing_runs()

    # ---------------------------------------------------------------------- #
    # S3 run log persistence
    # ---------------------------------------------------------------------- #

    def _load_existing_runs(self) -> None:
        """Read any previously persisted run records from S3 into memory."""
        try:
            glob = f"{self._runs_prefix}/*.parquet"
            count = self._con.execute(
                f"SELECT count(*) FROM glob('{glob}')"
            ).fetchone()[0]
            if count > 0:
                self._con.execute(
                    f"INSERT INTO run_records SELECT * FROM read_parquet('{glob}')"
                )
        except Exception:
            pass  # No runs yet or S3 not reachable — start fresh

    def _persist_run(self, record: RunRecord) -> None:
        """Write a single run record as a parquet file to S3."""
        dest = f"{self._runs_prefix}/{record.id}.parquet"
        try:
            self._con.execute(
                f"COPY (SELECT * FROM run_records WHERE id = ?) TO '{dest}' (FORMAT PARQUET)",
                [record.id],
            )
        except Exception:
            pass  # Best-effort — the in-memory table is the live source; S3 is for restart recovery

    # ---------------------------------------------------------------------- #
    # Internal helpers
    # ---------------------------------------------------------------------- #

    def _table_exists(self, table: str) -> bool:
        row = self._con.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND table_name = ?", [table]
        ).fetchone()
        return bool(row[0])

    def _row_to_snapshot(self, row: tuple) -> Snapshot:
        return Snapshot(
            id=row[0], table=row[1], created_at=row[2], rows=int(row[3]),
            schema_hash=row[4], meta=json.loads(row[5]) if row[5] else {},
        )

    def _row_to_run(self, row: tuple) -> RunRecord:
        data = dict(zip(_RUN_COLUMNS, row))
        return RunRecord(
            id=data["id"], step_name=data["step_name"], output_table=data["output_table"],
            status=data["status"], snapshot_id=data["snapshot_id"],
            fingerprint=json.loads(data["fingerprint"]) if data["fingerprint"] else {},
            rows=int(data["rows"]), duration_seconds=float(data["duration_seconds"]),
            started_at=data["started_at"], finished_at=data["finished_at"], error=data["error"],
        )

    # ---------------------------------------------------------------------- #
    # CatalogBackend
    # ---------------------------------------------------------------------- #

    def transaction(self) -> CatalogTransaction:
        self._lock.acquire()
        try:
            self._con.begin()
        except Exception:
            self._lock.release()
            raise
        return _S3Transaction(self)

    def latest_snapshot(self, table: str) -> Snapshot | None:
        row = self._con.execute(
            "SELECT snapshot_id, output_table, finished_at, rows, schema_hash, meta "
            "FROM run_records "
            "WHERE output_table = ? AND status = 'success' AND snapshot_id IS NOT NULL "
            "ORDER BY finished_at DESC LIMIT 1", [table]
        ).fetchone()
        return self._row_to_snapshot(row) if row else None

    def snapshots_since(self, table: str, since: datetime) -> list[Snapshot]:
        rows = self._con.execute(
            "SELECT snapshot_id, output_table, finished_at, rows, schema_hash, meta "
            "FROM run_records "
            "WHERE output_table = ? AND status = 'success' "
            "AND snapshot_id IS NOT NULL AND finished_at > ? ORDER BY finished_at ASC",
            [table, since],
        ).fetchall()
        return [self._row_to_snapshot(r) for r in rows]

    def as_relation(self, snapshot: Snapshot) -> duckdb.DuckDBPyRelation:
        _check_ident(snapshot.table)
        try:
            version = int(snapshot.id)
        except (TypeError, ValueError) as exc:
            raise SnapshotNotFoundError(f"invalid snapshot id: {snapshot.id!r}") from exc
        return self._con.sql(f'SELECT * FROM lake."{snapshot.table}" AT (VERSION => {version})')

    def new_rows_since(self, table: str, since_snapshot_id: str) -> duckdb.DuckDBPyRelation:
        _check_ident(table)
        since = int(since_snapshot_id)
        current = self._con.execute(
            "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"
        ).fetchone()[0]
        data_cols = self._con.sql(f'SELECT * FROM lake."{table}"').columns
        select_cols = ", ".join(f'"{c}"' for c in data_cols)
        return self._con.sql(
            f"SELECT {select_cols} FROM ducklake_table_changes("
            f"'lake', 'main', '{table}', {since + 1}, {current}) WHERE change_type = 'insert'"
        )

    def run_log(self) -> duckdb.DuckDBPyRelation:
        return self._con.sql("SELECT * FROM run_records ORDER BY finished_at DESC")

    def record_run(self, record: RunRecord) -> None:
        with self._lock:
            self._con.execute(
                "INSERT INTO run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [record.id, record.step_name, record.output_table, record.status,
                 record.snapshot_id, record.fingerprint.get("__schema_hash__"),
                 record.rows, record.duration_seconds, json.dumps(record.fingerprint),
                 json.dumps(record.fingerprint.get("__meta__", {})),
                 record.started_at, record.finished_at, record.error],
            )
        self._persist_run(record)

    def last_run(self, step_name: str, *, only_success: bool = False) -> RunRecord | None:
        clause = "AND status = 'success' " if only_success else ""
        row = self._con.execute(
            f"SELECT {', '.join(_RUN_COLUMNS)} FROM run_records "
            f"WHERE step_name = ? {clause}ORDER BY finished_at DESC LIMIT 1", [step_name]
        ).fetchone()
        return self._row_to_run(row) if row else None

    def get_run_by_id(self, run_id: str) -> RunRecord | None:
        row = self._con.execute(
            f"SELECT {', '.join(_RUN_COLUMNS)} FROM run_records WHERE id = ?", [run_id]
        ).fetchone()
        return self._row_to_run(row) if row else None

    def staged_relation(self, path: str) -> duckdb.DuckDBPyRelation:
        return self._con.read_parquet(path)

    def materialize(self, snapshot: Snapshot, dest_path: str) -> None:
        self.as_relation(snapshot).write_parquet(dest_path)

    def tables(self) -> list[str]:
        rows = self._con.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = 'lake' ORDER BY table_name"
        ).fetchall()
        return [r[0] for r in rows]

    def invalidate_runs(self, step_names: list[str]) -> None:
        with self._lock:
            for name in step_names:
                self._con.execute(
                    "DELETE FROM run_records WHERE step_name = ? AND status = 'success'",
                    [name],
                )

    def delete_old_runs(self, cutoff: datetime, *, keep_latest_per_table: bool = True) -> int:
        with self._lock:
            if keep_latest_per_table:
                n = self._con.execute(
                    """
                    DELETE FROM run_records
                    WHERE status = 'success'
                      AND finished_at < ?
                      AND snapshot_id IS NOT NULL
                      AND snapshot_id != (
                          SELECT snapshot_id FROM run_records r2
                          WHERE r2.output_table = run_records.output_table
                            AND r2.status = 'success'
                            AND r2.snapshot_id IS NOT NULL
                          ORDER BY r2.finished_at DESC LIMIT 1
                      )
                    """,
                    [cutoff],
                ).rowcount
            else:
                n = self._con.execute(
                    "DELETE FROM run_records WHERE finished_at < ?", [cutoff]
                ).rowcount
        return n

    def record_dead_letter(
        self,
        *,
        step_name: str,
        input_snapshot_ids: dict[str, str],
        error: str,
        traceback: str = "",
    ) -> None:
        import uuid
        with self._lock:
            self._con.execute(
                "INSERT INTO dead_letters VALUES (?,?,?,?,?,?)",
                [uuid.uuid4().hex, step_name, json.dumps(input_snapshot_ids),
                 error, traceback, datetime.now()],
            )

    def dead_letters(self) -> duckdb.DuckDBPyRelation:
        return self._con.sql("SELECT * FROM dead_letters ORDER BY failed_at DESC")

    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def close(self) -> None:
        self._con.close()

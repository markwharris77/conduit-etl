"""LocalCatalog — DuckLake on the local filesystem.

Data tables are stored in a DuckLake catalog, which gives ACID writes and
time-travel for free (every commit is a snapshot, queryable with
``AT (VERSION => n)``). A small sidecar DuckDB database (``runs.duckdb``) holds
the run log and acts as the index that maps a table's latest write to its
DuckLake snapshot version plus the metadata we attach to it.

Writes are serialised by a process-wide lock so that catalog commits never
interleave — matching the runtime's "serialise catalog commits" contract.
"""

from __future__ import annotations

import json
import re
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from types import TracebackType

import duckdb

from conduit_etl.catalog.base import CatalogBackend, CatalogTransaction
from conduit_etl.core.errors import CatalogError, SnapshotNotFoundError
from conduit_etl.core.models import MergeMode, RunRecord, Snapshot
from conduit_etl.core.fingerprint import schema_hash

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_RUN_COLUMNS = (
    "id",
    "step_name",
    "output_table",
    "status",
    "snapshot_id",
    "schema_hash",
    "rows",
    "duration_seconds",
    "fingerprint",
    "meta",
    "started_at",
    "finished_at",
    "error",
)


def _check_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise CatalogError(f"unsafe table identifier: {name!r}")
    return name


class _LocalTransaction(CatalogTransaction):
    def __init__(self, catalog: LocalCatalog) -> None:
        self._catalog = catalog
        self._pending: list[Snapshot] = []
        self._committed = False
        self._closed = False

    def write(
        self, table: str, relation: duckdb.DuckDBPyRelation, meta: dict
    ) -> Snapshot:
        _check_ident(table)
        con = self._catalog._con
        merge = MergeMode(meta.get("merge", MergeMode.REPLACE.value))
        merge_key = meta.get("merge_key")

        # Relations from a foreign connection can't be registered directly.
        # Materialise via a temp parquet file so the catalog connection can read it.
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        relation.write_parquet(tmp_path)
        stage_rel = con.read_parquet(tmp_path)
        con.register("_conduit_stage", stage_rel)
        try:
            exists = self._catalog._table_exists(table)
            if merge is MergeMode.REPLACE or not exists:
                con.execute(
                    f'CREATE OR REPLACE TABLE lake."{table}" AS '
                    "SELECT * FROM _conduit_stage"
                )
            elif merge is MergeMode.APPEND:
                con.execute(f'INSERT INTO lake."{table}" SELECT * FROM _conduit_stage')
            elif merge is MergeMode.UPSERT:
                if not merge_key:
                    raise CatalogError("upsert merge requires merge_key")
                keys = [_check_ident(k) for k in merge_key]
                on = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
                con.execute(
                    f'DELETE FROM lake."{table}" AS t '
                    f"USING _conduit_stage AS s WHERE {on}"
                )
                con.execute(f'INSERT INTO lake."{table}" SELECT * FROM _conduit_stage')
        finally:
            con.unregister("_conduit_stage")
            Path(tmp_path).unlink(missing_ok=True)

        rows = int(relation.aggregate("count(*) AS n").fetchone()[0])
        snap = Snapshot(
            id="",  # assigned on commit
            table=table,
            created_at=datetime.now(),
            rows=rows,
            schema_hash=schema_hash(relation),
            meta=dict(meta),
        )
        self._pending.append(snap)
        return snap

    def commit(self) -> None:
        if self._closed:
            return
        con = self._catalog._con
        con.commit()
        version = con.execute(
            "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"
        ).fetchone()[0]
        now = datetime.now()
        for snap in self._pending:
            snap.id = str(version)
            snap.created_at = now
        self._committed = True
        self._finish()

    def rollback(self) -> None:
        if self._closed:
            return
        self._catalog._con.rollback()
        self._finish()

    def _finish(self) -> None:
        self._closed = True
        self._catalog._lock.release()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self._closed:
            self.rollback()


class LocalCatalog(CatalogBackend):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.data_path = self.path / "data"
        self.path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        self._con = duckdb.connect()
        self._con.execute("INSTALL ducklake")
        self._con.execute("LOAD ducklake")
        catalog_file = self.path / "catalog.ducklake"
        self._con.execute(
            f"ATTACH 'ducklake:{catalog_file}' AS lake "
            f"(DATA_PATH '{self.data_path}')"
        )
        runs_file = self.path / "runs.duckdb"
        self._con.execute(f"ATTACH '{runs_file}' AS runs")
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS runs.run_records ("
            "id VARCHAR, step_name VARCHAR, output_table VARCHAR, status VARCHAR, "
            "snapshot_id VARCHAR, schema_hash VARCHAR, rows BIGINT, "
            "duration_seconds DOUBLE, fingerprint VARCHAR, meta VARCHAR, "
            "started_at TIMESTAMP, finished_at TIMESTAMP, error VARCHAR)"
        )

    # -- internal helpers ---------------------------------------------------- #

    def _table_exists(self, table: str) -> bool:
        row = self._con.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND table_name = ?",
            [table],
        ).fetchone()
        return bool(row[0])

    def _row_to_snapshot(self, row: tuple) -> Snapshot:
        # row order: snapshot_id, output_table, finished_at, rows, schema_hash, meta
        return Snapshot(
            id=row[0],
            table=row[1],
            created_at=row[2],
            rows=int(row[3]),
            schema_hash=row[4],
            meta=json.loads(row[5]) if row[5] else {},
        )

    def _row_to_run(self, row: tuple) -> RunRecord:
        data = dict(zip(_RUN_COLUMNS, row))
        return RunRecord(
            id=data["id"],
            step_name=data["step_name"],
            output_table=data["output_table"],
            status=data["status"],
            snapshot_id=data["snapshot_id"],
            fingerprint=json.loads(data["fingerprint"]) if data["fingerprint"] else {},
            rows=int(data["rows"]),
            duration_seconds=float(data["duration_seconds"]),
            started_at=data["started_at"],
            finished_at=data["finished_at"],
            error=data["error"],
        )

    # -- CatalogBackend ------------------------------------------------------ #

    def transaction(self) -> CatalogTransaction:
        self._lock.acquire()
        try:
            self._con.begin()
        except Exception:
            self._lock.release()
            raise
        return _LocalTransaction(self)

    def latest_snapshot(self, table: str) -> Snapshot | None:
        row = self._con.execute(
            "SELECT snapshot_id, output_table, finished_at, rows, schema_hash, meta "
            "FROM runs.run_records "
            "WHERE output_table = ? AND status = 'success' AND snapshot_id IS NOT NULL "
            "ORDER BY finished_at DESC LIMIT 1",
            [table],
        ).fetchone()
        return self._row_to_snapshot(row) if row else None

    def snapshots_since(self, table: str, since: datetime) -> list[Snapshot]:
        rows = self._con.execute(
            "SELECT snapshot_id, output_table, finished_at, rows, schema_hash, meta "
            "FROM runs.run_records "
            "WHERE output_table = ? AND status = 'success' "
            "AND snapshot_id IS NOT NULL AND finished_at > ? "
            "ORDER BY finished_at ASC",
            [table, since],
        ).fetchall()
        return [self._row_to_snapshot(r) for r in rows]

    def as_relation(self, snapshot: Snapshot) -> duckdb.DuckDBPyRelation:
        _check_ident(snapshot.table)
        try:
            version = int(snapshot.id)
        except (TypeError, ValueError) as exc:
            raise SnapshotNotFoundError(f"invalid snapshot id: {snapshot.id!r}") from exc
        return self._con.sql(
            f'SELECT * FROM lake."{snapshot.table}" AT (VERSION => {version})'
        )

    def new_rows_since(self, table: str, since_snapshot_id: str) -> duckdb.DuckDBPyRelation:
        _check_ident(table)
        since = int(since_snapshot_id)
        current = self._con.execute(
            "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"
        ).fetchone()[0]
        data_cols = [
            c
            for c in self._con.sql(f'SELECT * FROM lake."{table}"').columns
        ]
        select_cols = ", ".join(f'"{c}"' for c in data_cols)
        return self._con.sql(
            f"SELECT {select_cols} FROM ducklake_table_changes("
            f"'lake', 'main', '{table}', {since + 1}, {current}) "
            "WHERE change_type = 'insert'"
        )

    def run_log(self) -> duckdb.DuckDBPyRelation:
        return self._con.sql("SELECT * FROM runs.run_records ORDER BY finished_at DESC")

    def record_run(self, record: RunRecord) -> None:
        with self._lock:
            self._con.execute(
                "INSERT INTO runs.run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    record.id,
                    record.step_name,
                    record.output_table,
                    record.status,
                    record.snapshot_id,
                    record.fingerprint.get("__schema_hash__"),
                    record.rows,
                    record.duration_seconds,
                    json.dumps(record.fingerprint),
                    json.dumps(record.fingerprint.get("__meta__", {})),
                    record.started_at,
                    record.finished_at,
                    record.error,
                ],
            )

    def last_run(self, step_name: str, *, only_success: bool = False) -> RunRecord | None:
        clause = "AND status = 'success' " if only_success else ""
        row = self._con.execute(
            f"SELECT {', '.join(_RUN_COLUMNS)} FROM runs.run_records "
            f"WHERE step_name = ? {clause}"
            "ORDER BY finished_at DESC LIMIT 1",
            [step_name],
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

    def connection(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection (catalog attached as ``lake``)."""
        return self._con

    def close(self) -> None:
        self._con.close()

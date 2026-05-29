"""CatalogBackend — where pipeline data and run history live.

The catalog is the only durable source of truth in conduit-etl. A backend stores
table data as versioned snapshots (time-travel) and keeps a run log. Application
code only ever talks to this abstract interface, never a concrete backend.
"""

from __future__ import annotations

import tempfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from types import TracebackType

import duckdb

from conduit_etl.core.errors import CatalogError
from conduit_etl.core.models import MergeMode, RunRecord, Snapshot


def write_relation_to_lake(
    con: duckdb.DuckDBPyConnection,
    table: str,
    relation: duckdb.DuckDBPyRelation,
    merge: MergeMode,
    merge_key: list[str] | None,
    table_exists: bool,
) -> None:
    """Write ``relation`` into the DuckLake table ``table`` under ``con``.

    This is the shared write path for both LocalCatalog and S3Catalog. It
    materialises ``relation`` to a temp parquet file first to avoid cross-
    connection restrictions, then performs the requested merge strategy.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    relation.write_parquet(tmp_path)
    stage_rel = con.read_parquet(tmp_path)
    con.register("_conduit_stage", stage_rel)
    try:
        if merge is MergeMode.REPLACE or not table_exists:
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
            on = " AND ".join(f't."{k}" = s."{k}"' for k in merge_key)
            con.execute(f'DELETE FROM lake."{table}" AS t USING _conduit_stage AS s WHERE {on}')
            con.execute(f'INSERT INTO lake."{table}" SELECT * FROM _conduit_stage')
    finally:
        con.unregister("_conduit_stage")
        Path(tmp_path).unlink(missing_ok=True)


class CatalogTransaction(ABC):
    """A unit of atomic catalog writes.

    Use as a context manager. Writes are only durable once :meth:`commit` is
    called; leaving the block without committing rolls back.
    """

    @abstractmethod
    def write(self, table: str, relation: duckdb.DuckDBPyRelation, meta: dict) -> Snapshot:
        """Write ``relation`` as a new snapshot of ``table``.

        The returned :class:`Snapshot` is finalised (its ``id`` assigned) on
        :meth:`commit`. Callers should treat the id as valid only after commit.
        """

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    def __enter__(self) -> CatalogTransaction:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is not None:
            self.rollback()


class CatalogBackend(ABC):
    """Abstract catalog backend.

    All methods marked ``@abstractmethod`` must be implemented by every backend.
    Methods without the decorator have sensible defaults and may be overridden.
    """

    @abstractmethod
    def transaction(self) -> CatalogTransaction: ...

    @abstractmethod
    def latest_snapshot(self, table: str) -> Snapshot | None: ...

    @abstractmethod
    def snapshots_since(self, table: str, since: datetime) -> list[Snapshot]: ...

    @abstractmethod
    def as_relation(self, snapshot: Snapshot) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation pointing at this snapshot's data."""

    @abstractmethod
    def new_rows_since(
        self, table: str, since_snapshot_id: str
    ) -> duckdb.DuckDBPyRelation:
        """For incremental steps — rows added after a given snapshot."""

    @abstractmethod
    def run_log(self) -> duckdb.DuckDBPyRelation:
        """Query the run history as a DuckDB relation."""

    @abstractmethod
    def record_run(self, record: RunRecord) -> None:
        """Persist one execution to the run log."""

    @abstractmethod
    def last_run(self, step_name: str, *, only_success: bool = False) -> RunRecord | None:
        """The most recent run record for a step, optionally only successes."""

    @abstractmethod
    def get_run_by_id(self, run_id: str) -> RunRecord | None:
        """Fetch a specific run record by its ID."""

    @abstractmethod
    def staged_relation(self, path: str) -> duckdb.DuckDBPyRelation:
        """Read a staging parquet file into a relation on the catalog connection."""

    @abstractmethod
    def materialize(self, snapshot: Snapshot, dest_path: str) -> None:
        """Write a snapshot's data out to a parquet file (executor input handoff)."""

    @abstractmethod
    def tables(self) -> list[str]:
        """Names of all data tables known to the catalog."""

    @abstractmethod
    def invalidate_runs(self, step_names: list[str]) -> None:
        """Delete success run records for the given steps (forces re-run on next tick)."""

    @abstractmethod
    def delete_old_runs(
        self, cutoff: datetime, *, keep_latest_per_table: bool = True
    ) -> int:
        """Delete run records older than ``cutoff``. Returns count deleted."""

    # ------------------------------------------------------------------ #
    # Optional — backends may override for richer behaviour
    # ------------------------------------------------------------------ #

    def record_dead_letter(
        self,
        *,
        step_name: str,
        input_snapshot_ids: dict[str, str],
        error: str,
        traceback: str = "",
    ) -> None:
        """Persist a failed execution to the dead-letter store.

        Default implementation is a no-op. Override to enable dead-letter tracking.
        """

    def dead_letters(self) -> duckdb.DuckDBPyRelation:
        """Return all dead-letter records as a DuckDB relation.

        Default returns an empty relation. Override if the backend stores dead letters.
        """
        return duckdb.sql(
            "SELECT '' AS id, '' AS step_name, '' AS input_snapshot_ids, "
            "'' AS error, '' AS traceback, CURRENT_TIMESTAMP AS failed_at LIMIT 0"
        )

    def close(self) -> None:
        """Release any resources held by this backend."""

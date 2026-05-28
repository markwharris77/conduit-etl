"""CatalogBackend — where pipeline data and run history live.

The catalog is the only durable source of truth in conduit-etl. A backend stores
table data as versioned snapshots (time-travel) and keeps a run log. Application
code only ever talks to this abstract interface, never a concrete backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from types import TracebackType

import duckdb

from conduit_etl.core.models import RunRecord, Snapshot


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
    """Abstract catalog backend."""

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
    def staged_relation(self, path: str) -> duckdb.DuckDBPyRelation:
        """Read a staging parquet file into a relation on the catalog connection."""

    @abstractmethod
    def materialize(self, snapshot: Snapshot, dest_path: str) -> None:
        """Write a snapshot's data out to a parquet file (executor input handoff)."""

    @abstractmethod
    def tables(self) -> list[str]:
        """Names of all data tables known to the catalog."""

    def close(self) -> None:  # pragma: no cover - trivial default
        """Release any resources. Default no-op."""

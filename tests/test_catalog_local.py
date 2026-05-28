"""Integration tests for LocalCatalog — uses a real DuckLake on tmp_path."""

import uuid
import duckdb
import pytest
from datetime import datetime

from conduit_etl.catalog.local import LocalCatalog
from conduit_etl.core.models import RunRecord


@pytest.fixture
def catalog(tmp_path):
    cat = LocalCatalog(tmp_path / "catalog")
    yield cat
    cat.close()


def _relation(sql: str) -> duckdb.DuckDBPyRelation:
    return duckdb.sql(sql)


def _record_success(catalog, snap, step_name=None, fingerprint=None):
    """Helper: write a success run record for a snapshot (mirrors what Runtime does)."""
    now = datetime.now()
    catalog.record_run(RunRecord(
        id=uuid.uuid4().hex,
        step_name=step_name or snap.table,
        output_table=snap.table,
        status="success",
        snapshot_id=snap.id,
        fingerprint=fingerprint or {"__fn_hash__": "abc"},
        rows=snap.rows,
        duration_seconds=0.1,
        started_at=now,
        finished_at=now,
        error=None,
    ))


def test_write_and_read_snapshot(catalog):
    rel = _relation("SELECT 1 AS id, 'alice' AS name")
    with catalog.transaction() as txn:
        snap = txn.write("users", rel, {"merge": "replace"})
        txn.commit()
    _record_success(catalog, snap)

    assert snap.id != ""
    assert snap.table == "users"
    assert snap.rows == 1

    retrieved = catalog.latest_snapshot("users")
    assert retrieved is not None
    assert retrieved.id == snap.id


def test_as_relation(catalog):
    rel = _relation("SELECT 42 AS val")
    with catalog.transaction() as txn:
        snap = txn.write("vals", rel, {"merge": "replace"})
        txn.commit()

    back = catalog.as_relation(snap)
    row = back.fetchone()
    assert row[0] == 42


def test_tables_lists_written_tables(catalog):
    with catalog.transaction() as txn:
        txn.write("t1", _relation("SELECT 1 AS x"), {"merge": "replace"})
        txn.commit()
    with catalog.transaction() as txn:
        txn.write("t2", _relation("SELECT 2 AS x"), {"merge": "replace"})
        txn.commit()

    assert "t1" in catalog.tables()
    assert "t2" in catalog.tables()


def test_append_merge(catalog):
    rel1 = _relation("SELECT 1 AS id")
    rel2 = _relation("SELECT 2 AS id")
    with catalog.transaction() as txn:
        snap1 = txn.write("nums", rel1, {"merge": "replace"})
        txn.commit()
    _record_success(catalog, snap1)
    with catalog.transaction() as txn:
        snap2 = txn.write("nums", rel2, {"merge": "append"})
        txn.commit()
    _record_success(catalog, snap2)

    snap = catalog.latest_snapshot("nums")
    rows = catalog.as_relation(snap).fetchall()
    assert len(rows) == 2


def test_record_and_query_run(catalog):
    now = datetime.now()
    rec = RunRecord(
        id="test-id",
        step_name="my_step",
        output_table="my_step",
        status="success",
        snapshot_id="1",
        fingerprint={"__fn_hash__": "abc"},
        rows=10,
        duration_seconds=1.5,
        started_at=now,
        finished_at=now,
        error=None,
    )
    catalog.record_run(rec)

    last = catalog.last_run("my_step")
    assert last is not None
    assert last.status == "success"
    assert last.rows == 10


def test_snapshots_since(catalog):
    from datetime import timedelta

    before = datetime.now() - timedelta(seconds=1)
    with catalog.transaction() as txn:
        snap = txn.write("tbl", _relation("SELECT 1 AS x"), {"merge": "replace"})
        txn.commit()
    _record_success(catalog, snap)

    snaps = catalog.snapshots_since("tbl", before)
    assert len(snaps) >= 1


def test_rollback_leaves_no_snapshot(catalog):
    rel = _relation("SELECT 99 AS x")
    with catalog.transaction() as txn:
        txn.write("rollback_test", rel, {"merge": "replace"})
        txn.rollback()

    snap = catalog.latest_snapshot("rollback_test")
    assert snap is None


def test_unsafe_table_name_rejected(catalog):
    from conduit_etl.core.errors import CatalogError

    rel = _relation("SELECT 1 AS x")
    with catalog.transaction() as txn:
        with pytest.raises(CatalogError):
            txn.write("bad; DROP TABLE x--", rel, {"merge": "replace"})
        txn.rollback()

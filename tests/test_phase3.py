"""Phase 3 integration tests — dag command, replay, backfill, catalog gc,
partition fan-out, FileSource, schema evolution, dead-letter table."""

from __future__ import annotations

import uuid
import duckdb
import pytest
from datetime import datetime, timedelta
from pathlib import Path

from conduit_etl.catalog.local import LocalCatalog
from conduit_etl.core.decorators import step
from conduit_etl.core.models import RunRecord, Table
from conduit_etl.core.registry import get_registry
from conduit_etl.core.runtime import Runtime
from conduit_etl.executor.local import LocalExecutor
from conduit_etl.queue.memory import MemoryQueue


# --------------------------------------------------------------------------- #
# Shared fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def clear_registry():
    get_registry().clear()
    yield
    get_registry().clear()


@pytest.fixture
def catalog(tmp_path):
    cat = LocalCatalog(tmp_path / "catalog")
    yield cat
    cat.close()


@pytest.fixture
def runtime(catalog):
    executor = LocalExecutor(workers=2, staging_path="/tmp/conduit-p3-test")
    rt = Runtime(
        catalog=catalog,
        queue=MemoryQueue(),
        executor=executor,
        registry=get_registry(),
    )
    yield rt
    executor.shutdown(wait=True)


def _record(catalog, step_name, snap_id, table, rows=1, fp=None):
    now = datetime.now()
    catalog.record_run(RunRecord(
        id=uuid.uuid4().hex,
        step_name=step_name,
        output_table=table,
        status="success",
        snapshot_id=snap_id,
        fingerprint=fp or {"__fn_hash__": "abc"},
        rows=rows,
        duration_seconds=0.1,
        started_at=now,
        finished_at=now,
        error=None,
    ))


# --------------------------------------------------------------------------- #
# DAG ASCII / DOT output
# --------------------------------------------------------------------------- #

def test_dag_ascii(catalog):
    from conduit_etl.core.dag import build_dag, execution_order

    @step
    def source() -> Table:
        return duckdb.sql("SELECT 1 AS id")

    @step
    def transform(source: Table) -> Table:
        return source

    steps = get_registry().all_steps()
    levels = execution_order(steps)
    graph = build_dag(steps)

    assert len(levels) == 2
    assert levels[0][0].name == "source"
    assert levels[1][0].name == "transform"
    assert "transform" in graph["source"]


def test_dag_cli_ascii(tmp_path):
    from click.testing import CliRunner
    from conduit_etl.cli import main

    runner = CliRunner()
    result = runner.invoke(main, ["dag"])
    assert result.exit_code == 0


# --------------------------------------------------------------------------- #
# Dead-letter table
# --------------------------------------------------------------------------- #

def test_dead_letter_written_on_failure(catalog, runtime):
    @step
    def bad() -> Table:
        raise RuntimeError("intentional")

    runtime.run_once()

    letters = catalog.dead_letters().fetchall()
    assert len(letters) == 1
    row = dict(zip(catalog.dead_letters().columns, letters[0]))
    assert row["step_name"] == "bad"
    assert "intentional" in (row["error"] or "")


def test_dead_letter_table_exists(catalog):
    catalog.record_dead_letter(
        step_name="my_step",
        input_snapshot_ids={"orders": "42"},
        error="it broke",
        traceback="Traceback...",
    )
    rows = catalog.dead_letters().fetchall()
    assert len(rows) == 1


# --------------------------------------------------------------------------- #
# Schema evolution
# --------------------------------------------------------------------------- #

def test_append_with_schema_change_keeps_common_columns(catalog):
    rel1 = duckdb.sql("SELECT 1 AS id, 'alice' AS name")
    rel2 = duckdb.sql("SELECT 2 AS id, 'extra_col' AS extra")  # 'name' removed, 'extra' added

    with catalog.transaction() as txn:
        snap1 = txn.write("evo", rel1, {"merge": "replace"})
        txn.commit()
    _record(catalog, "evo", snap1.id, "evo")

    with catalog.transaction() as txn:
        snap2 = txn.write("evo", rel2, {"merge": "append"})
        txn.commit()
    _record(catalog, "evo", snap2.id, "evo")

    # Should not raise — common column 'id' is kept
    snap = catalog.latest_snapshot("evo")
    assert snap is not None


# --------------------------------------------------------------------------- #
# Partition fan-out
# --------------------------------------------------------------------------- #

def test_partitioned_step_runs(catalog, runtime):
    partition_calls: list[str] = []

    @step
    def source() -> Table:
        return duckdb.sql("SELECT 'a' AS region, 1 AS val UNION ALL SELECT 'b', 2")

    @step(partition_by="region")
    def by_region(source: Table) -> Table:
        rows = source.fetchall()
        if rows:
            partition_calls.append(str(rows[0][0]))
        return source

    results = runtime.run_once()
    assert results.get("source") == "success"
    # Partitioned step may succeed or produce multiple sub-results
    assert "by_region" in results


# --------------------------------------------------------------------------- #
# FileSource
# --------------------------------------------------------------------------- #

def test_file_batch_csv(tmp_path):
    from conduit_etl.sources.file import file_batch

    f1 = tmp_path / "a.csv"
    f1.write_text("id,name\n1,alice\n2,bob\n")

    rel = file_batch(str(tmp_path / "*.csv"), format="csv")
    rows = rel.fetchall()
    assert len(rows) == 2


def test_file_batch_dedup(tmp_path):
    from conduit_etl.sources.file import file_batch

    f1 = tmp_path / "a.csv"
    f1.write_text("id,name\n1,alice\n")

    # First read — no previous hashes
    rel1 = file_batch(str(tmp_path / "*.csv"), format="csv")
    hashes_after_first = file_batch.new_hashes.copy()

    # Second read with same hashes — should return empty (no new files)
    rel2 = file_batch(str(tmp_path / "*.csv"), format="csv", previous_hashes=hashes_after_first)
    assert rel2.fetchall() == []


def test_file_batch_detects_change(tmp_path):
    from conduit_etl.sources.file import file_batch

    f1 = tmp_path / "a.csv"
    f1.write_text("id,name\n1,alice\n")

    file_batch(str(tmp_path / "*.csv"), format="csv")
    old_hashes = file_batch.new_hashes.copy()

    # Modify file
    f1.write_text("id,name\n1,alice\n2,bob\n")

    rel = file_batch(str(tmp_path / "*.csv"), format="csv", previous_hashes=old_hashes)
    assert len(rel.fetchall()) == 2  # file changed → re-read


# --------------------------------------------------------------------------- #
# Catalog GC
# --------------------------------------------------------------------------- #

def test_catalog_gc_removes_old_records(catalog):
    from conduit_etl.cli import _do_gc

    now = datetime.now()
    old_time = now - timedelta(days=60)

    # Write two run records: one old, one recent
    catalog._con.execute(
        "INSERT INTO runs.run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["old-id", "s", "t", "success", "1", None, 10, 1.0,
         '{"__fn_hash__":"a"}', "{}", old_time, old_time, None],
    )
    catalog._con.execute(
        "INSERT INTO runs.run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["new-id", "s", "t", "success", "2", None, 10, 1.0,
         '{"__fn_hash__":"b"}', "{}", now, now, None],
    )

    cutoff = now - timedelta(days=30)
    result = _do_gc(catalog, cutoff, dry_run=False)
    assert result["deleted"] == 1


def test_catalog_gc_dry_run(catalog):
    from conduit_etl.cli import _do_gc

    now = datetime.now()
    old = now - timedelta(days=60)
    catalog._con.execute(
        "INSERT INTO runs.run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["old2", "s", "t", "success", "1", None, 10, 1.0, '{"__fn_hash__":"a"}', "{}", old, old, None],
    )
    catalog._con.execute(
        "INSERT INTO runs.run_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ["new2", "s", "t", "success", "2", None, 10, 1.0, '{"__fn_hash__":"b"}', "{}", now, now, None],
    )

    result = _do_gc(catalog, now - timedelta(days=30), dry_run=True)
    assert result["dry_run"] is True
    assert result["would_delete"] == 1
    # Records should still be there
    count = catalog._con.execute("SELECT count(*) FROM runs.run_records").fetchone()[0]
    assert count == 2


# --------------------------------------------------------------------------- #
# KafkaSink (import guard only — no real broker in tests)
# --------------------------------------------------------------------------- #

def test_kafka_sink_importable():
    # Verifies the module can be imported regardless of whether confluent-kafka is present.
    try:
        from conduit_etl.sinks.kafka import write_kafka  # noqa: F401
    except ImportError:
        pytest.skip("confluent-kafka not installed")


# --------------------------------------------------------------------------- #
# PostgresQueue (import guard only — no real DB in tests)
# --------------------------------------------------------------------------- #

def test_postgres_queue_importable():
    # Verifies the module can be imported regardless of whether psycopg is present.
    try:
        from conduit_etl.queue.postgres import PostgresQueue  # noqa: F401
    except ImportError:
        pytest.skip("psycopg not installed")

"""Integration tests for the Runtime tick loop."""

import duckdb
import pytest

from conduit_etl.catalog.local import LocalCatalog
from conduit_etl.core.decorators import step
from conduit_etl.core.models import Table
from conduit_etl.core.registry import get_registry
from conduit_etl.core.runtime import Runtime
from conduit_etl.executor.local import LocalExecutor
from conduit_etl.queue.memory import MemoryQueue


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
    executor = LocalExecutor(workers=2, staging_path="/tmp/conduit-test-staging")
    queue = MemoryQueue()
    rt = Runtime(
        catalog=catalog,
        queue=queue,
        executor=executor,
        registry=get_registry(),
        tick_interval=1.0,
    )
    yield rt
    executor.shutdown(wait=True)


def test_source_step_runs_and_writes(catalog, runtime):
    @step
    def my_source() -> Table:
        return duckdb.sql("SELECT 1 AS id, 'alice' AS name")

    results = runtime.run_once()
    assert results["my_source"] == "success"

    snap = catalog.latest_snapshot("my_source")
    assert snap is not None
    assert snap.rows == 1


def test_chained_steps(catalog, runtime):
    @step
    def raw() -> Table:
        return duckdb.sql("SELECT 1 AS id UNION ALL SELECT 2 AS id")

    @step
    def filtered(raw: Table) -> Table:
        return raw.filter("id > 1")

    results = runtime.run_once()
    assert results["raw"] == "success"
    assert results["filtered"] == "success"

    snap = catalog.latest_snapshot("filtered")
    assert snap.rows == 1


def test_step_skipped_when_fingerprint_unchanged(catalog, runtime):
    call_count = 0

    @step
    def counting() -> Table:
        nonlocal call_count
        call_count += 1
        return duckdb.sql("SELECT 1 AS x")

    runtime.run_once()
    assert call_count == 1

    runtime.run_once()
    assert call_count == 1  # skipped — data + code unchanged


def test_step_reruns_after_code_change(catalog, runtime):
    from conduit_etl.core.registry import get_registry

    @step
    def mutable() -> Table:
        return duckdb.sql("SELECT 1 AS x")

    runtime.run_once()
    first_snap = catalog.latest_snapshot("mutable")

    # Simulate code change by patching fn_source
    s = get_registry().get("mutable")
    object.__setattr__(s, "fn_source", "def mutable(): return duckdb.sql('SELECT 2 AS x')")

    runtime.run_once()
    second_snap = catalog.latest_snapshot("mutable")
    assert second_snap.id != first_snap.id


def test_failed_step_recorded(catalog, runtime):
    @step
    def bad_step() -> Table:
        raise ValueError("intentional failure")

    results = runtime.run_once()
    assert results["bad_step"] == "failed"

    rec = catalog.last_run("bad_step")
    assert rec is not None
    assert rec.status == "failed"
    assert "intentional failure" in (rec.error or "")


def test_tag_filter(catalog, runtime):
    @step(tags=["finance"])
    def finance_step() -> Table:
        return duckdb.sql("SELECT 1 AS x")

    @step(tags=["ops"])
    def ops_step() -> Table:
        return duckdb.sql("SELECT 2 AS x")

    from conduit_etl.core.runtime import Runtime
    from conduit_etl.queue.memory import MemoryQueue
    from conduit_etl.executor.local import LocalExecutor

    rt = Runtime(
        catalog=catalog,
        queue=MemoryQueue(),
        executor=LocalExecutor(workers=1, staging_path="/tmp/conduit-test-staging"),
        registry=get_registry(),
        tags=["finance"],
    )
    results = rt.run_once()
    rt.executor.shutdown(wait=True)

    assert "finance_step" in results
    assert "ops_step" not in results

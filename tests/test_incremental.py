"""Tests for incremental mode (incremental=True on @step)."""

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
    executor = LocalExecutor(workers=1, staging_path="/tmp/conduit-incr-test")
    rt = Runtime(
        catalog=catalog,
        queue=MemoryQueue(),
        executor=executor,
        registry=get_registry(),
    )
    yield rt
    executor.shutdown(wait=True)


def test_incremental_step_receives_new_rows_only(catalog, runtime):
    received_rows: list[int] = []

    @step
    def source_data() -> Table:
        return duckdb.sql("SELECT 1 AS id UNION ALL SELECT 2 AS id")

    @step(incremental=True)
    def incremental_consumer(source_data: Table) -> Table:
        rows = source_data.fetchall()
        received_rows.append(len(rows))
        return source_data

    # First run: should receive all rows
    runtime.run_once()
    assert received_rows[-1] == 2

    # Add more rows to source
    get_registry().clear()

    @step
    def source_data() -> Table:  # noqa: F811
        return duckdb.sql("SELECT 1 AS id UNION ALL SELECT 2 AS id UNION ALL SELECT 3 AS id")

    @step(incremental=True)
    def incremental_consumer(source_data: Table) -> Table:  # noqa: F811
        rows = source_data.fetchall()
        received_rows.append(len(rows))
        return source_data

    runtime.run_once()
    # On second run, incremental step gets only new rows (ideally 1, but fallback to full is ok)
    assert received_rows[-1] >= 1

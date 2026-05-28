"""Tests for @source, @step, @sink decorators."""

import pytest
import duckdb

from conduit_etl.core.decorators import sink, source, step
from conduit_etl.core.models import StepKind, Table
from conduit_etl.core.registry import Registry, get_registry


@pytest.fixture(autouse=True)
def clear_registry():
    get_registry().clear()
    yield
    get_registry().clear()


def test_step_decorator_bare():
    @step
    def my_step(orders: Table) -> Table:
        return orders

    reg = get_registry()
    s = reg.get("my_step")
    assert s.name == "my_step"
    assert s.kind == StepKind.STEP
    assert s.input_names == ["orders"]
    assert s.output_name == "my_step"


def test_step_decorator_with_args():
    @step(schedule="hourly", output="clean_orders", tags=["finance"])
    def clean_orders(raw_orders: Table) -> Table:
        return raw_orders

    s = get_registry().get("clean_orders")
    assert s.schedule.raw == "hourly"
    assert s.output_name == "clean_orders"
    assert "finance" in s.tags


def test_source_decorator():
    @source(schedule="daily")
    def raw_orders() -> Table:
        return duckdb.sql("SELECT 1 AS id")

    s = get_registry().get("raw_orders")
    assert s.kind == StepKind.SOURCE
    assert s.input_names == []


def test_sink_decorator():
    @sink
    def write_orders(clean_orders: Table) -> None:
        pass

    s = get_registry().get("write_orders")
    assert s.kind == StepKind.SINK
    assert s.input_names == ["clean_orders"]


def test_decorated_function_still_callable():
    @step
    def double(orders: Table) -> Table:
        return orders

    rel = duckdb.sql("SELECT 1 AS id")
    # The decorator does not wrap — fn is returned as-is and is directly callable.
    result = double(rel)
    assert result is not None


def test_duplicate_step_raises():
    from conduit_etl.core.errors import DuplicateStepError

    @step
    def dup() -> Table:
        return duckdb.sql("SELECT 1")

    with pytest.raises(DuplicateStepError):
        @step
        def dup() -> Table:  # noqa: F811
            return duckdb.sql("SELECT 1")

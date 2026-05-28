"""Tests for DAG construction and topological level sort."""

import pytest

from conduit_etl.core.dag import build_dag, execution_order, topological_levels
from conduit_etl.core.errors import CycleError
from conduit_etl.core.models import Step, StepKind, Schedule


def _step(name: str, inputs: list[str], output: str | None = None) -> Step:
    return Step(
        name=name,
        fn=lambda: None,
        output_name=output or name,
        input_names=inputs,
        kind=StepKind.STEP,
        fn_source="",
        schedule=Schedule.parse(None),
    )


def test_build_dag_linear():
    steps = [
        _step("a", []),
        _step("b", ["a"]),
        _step("c", ["b"]),
    ]
    dag = build_dag(steps)
    assert dag["a"] == ["b"]
    assert dag["b"] == ["c"]
    assert dag["c"] == []


def test_build_dag_fan_out():
    steps = [
        _step("source", []),
        _step("b", ["source"]),
        _step("c", ["source"]),
    ]
    dag = build_dag(steps)
    assert set(dag["source"]) == {"b", "c"}


def test_build_dag_fan_in():
    steps = [
        _step("a", []),
        _step("b", []),
        _step("merge", ["a", "b"]),
    ]
    dag = build_dag(steps)
    assert "merge" in dag["a"]
    assert "merge" in dag["b"]


def test_build_dag_ignores_external_inputs():
    steps = [_step("a", ["external_table"])]
    dag = build_dag(steps)
    assert dag["a"] == []


def test_topological_levels_linear():
    graph = {"a": ["b"], "b": ["c"], "c": []}
    levels = topological_levels(graph)
    assert levels == [["a"], ["b"], ["c"]]


def test_topological_levels_parallel():
    graph = {"a": ["c"], "b": ["c"], "c": []}
    levels = topological_levels(graph)
    assert set(levels[0]) == {"a", "b"}
    assert levels[1] == ["c"]


def test_topological_levels_cycle():
    graph = {"a": ["b"], "b": ["a"]}
    with pytest.raises(CycleError):
        topological_levels(graph)


def test_execution_order_returns_steps():
    steps = [
        _step("a", []),
        _step("b", ["a"]),
    ]
    levels = execution_order(steps)
    assert len(levels) == 2
    assert levels[0][0].name == "a"
    assert levels[1][0].name == "b"

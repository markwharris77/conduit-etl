"""Tests for fingerprint computation and change detection."""

import duckdb
import pytest

from conduit_etl.core.fingerprint import (
    compute_fingerprint,
    fingerprint_changed,
    fn_hash,
    schema_hash,
)
from conduit_etl.core.models import Schedule, Step, StepKind


def _step(name: str, inputs: list[str]) -> Step:
    return Step(
        name=name,
        fn=lambda: None,
        output_name=name,
        input_names=inputs,
        kind=StepKind.STEP,
        fn_source="def foo(): pass",
        schedule=Schedule.parse(None),
    )


class _MockCatalog:
    def __init__(self, snapshots: dict):
        self._snaps = snapshots

    def latest_snapshot(self, table: str):
        return self._snaps.get(table)


class _MockSnapshot:
    def __init__(self, sid: str, rows: int):
        self.id = sid
        self.rows = rows


def test_fn_hash_is_stable():
    h1 = fn_hash("def foo(): pass")
    h2 = fn_hash("def foo(): pass")
    assert h1 == h2


def test_fn_hash_differs_on_change():
    h1 = fn_hash("def foo(): pass")
    h2 = fn_hash("def foo(): return 1")
    assert h1 != h2


def test_schema_hash_stable():
    rel = duckdb.sql("SELECT 1 AS a, 'x' AS b")
    h1 = schema_hash(rel)
    h2 = schema_hash(rel)
    assert h1 == h2


def test_compute_fingerprint_no_inputs():
    step = _step("s", [])
    catalog = _MockCatalog({})
    fp = compute_fingerprint(step, catalog)
    assert "__fn_hash__" in fp


def test_compute_fingerprint_with_inputs():
    step = _step("s", ["orders"])
    snap = _MockSnapshot("42", 100)
    catalog = _MockCatalog({"orders": snap})
    fp = compute_fingerprint(step, catalog)
    assert fp["orders"] == ["42", 100]


def test_compute_fingerprint_missing_input():
    step = _step("s", ["missing_table"])
    catalog = _MockCatalog({})
    fp = compute_fingerprint(step, catalog)
    assert fp["missing_table"] is None


def test_fingerprint_changed_none_previous():
    assert fingerprint_changed(None, {"__fn_hash__": "abc"})


def test_fingerprint_changed_same():
    fp = {"__fn_hash__": "abc", "orders": ["1", 10]}
    assert not fingerprint_changed(fp, fp)


def test_fingerprint_changed_data_change():
    old = {"__fn_hash__": "abc", "orders": ["1", 10]}
    new = {"__fn_hash__": "abc", "orders": ["2", 20]}
    assert fingerprint_changed(old, new)


def test_fingerprint_changed_code_change():
    old = {"__fn_hash__": "abc", "orders": ["1", 10]}
    new = {"__fn_hash__": "xyz", "orders": ["1", 10]}
    assert fingerprint_changed(old, new)


def test_fingerprint_ignores_meta_key():
    fp = {"__fn_hash__": "abc", "__meta__": {"extra": "stuff"}}
    same = {"__fn_hash__": "abc"}
    assert not fingerprint_changed(fp, same)

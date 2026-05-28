"""Tests for the step registry."""

import pytest

from conduit_etl.core.errors import DuplicateStepError, UnknownStepError
from conduit_etl.core.models import Schedule, Step, StepKind
from conduit_etl.core.registry import Registry


def _step(name: str) -> Step:
    return Step(
        name=name,
        fn=lambda: None,
        output_name=name,
        input_names=[],
        kind=StepKind.STEP,
        fn_source="",
        schedule=Schedule.parse(None),
    )


def test_register_and_get():
    reg = Registry()
    s = _step("foo")
    reg.register(s)
    assert reg.get("foo") is s


def test_get_unknown_raises():
    reg = Registry()
    with pytest.raises(UnknownStepError):
        reg.get("nope")


def test_duplicate_raises():
    reg = Registry()
    reg.register(_step("foo"))
    with pytest.raises(DuplicateStepError):
        reg.register(_step("foo"))


def test_all_steps():
    reg = Registry()
    reg.register(_step("a"))
    reg.register(_step("b"))
    names = {s.name for s in reg.all_steps()}
    assert names == {"a", "b"}


def test_clear():
    reg = Registry()
    reg.register(_step("a"))
    reg.clear()
    assert len(reg) == 0

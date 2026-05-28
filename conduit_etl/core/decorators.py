"""@source, @step, @sink decorators — the public API for defining pipeline work.

All three decorators wrap a plain Python function and register it with the global
registry. The wrapped function is preserved as ``fn.__wrapped__`` so tests can
call it directly without going through the decorator machinery.

DAG wiring is inferred from parameter names: a parameter named ``orders`` means
the step depends on the output table named ``orders``. Output name defaults to
the function name; ``@step(output="my_table")`` overrides it.
"""

from __future__ import annotations

import inspect
import textwrap
from collections.abc import Callable, Sequence
from typing import Any, TypeVar, overload

from conduit_etl.core.models import MergeMode, Schedule, Step, StepKind, Table, parse_duration
from conduit_etl.core.registry import get_registry

F = TypeVar("F", bound=Callable[..., Any])

_RESERVED = frozenset({"return", "self", "cls"})


def _input_names(fn: Callable[..., Any]) -> list[str]:
    """Parameter names that are DAG inputs.

    All parameters are treated as DAG inputs. Config and scheduling are
    expressed through the decorator kwargs, never as function parameters.
    If no producer exists for a parameter name, it is treated as an
    external dependency and the DAG simply ignores it.
    """
    sig = inspect.signature(fn)
    return [p for p in sig.parameters if p not in _RESERVED]


def _fn_source(fn: Callable[..., Any]) -> str:
    try:
        return textwrap.dedent(inspect.getsource(fn))
    except OSError:
        return ""


def _register(
    fn: Callable[..., Any],
    *,
    kind: StepKind,
    output: str | None,
    schedule: str | None,
    incremental: bool,
    merge: str,
    merge_key: Sequence[str] | None,
    partition_by: str | None,
    max_partitions: int,
    timeout: str,
    retry: int,
    retry_on: tuple[type[BaseException], ...],
    tags: list[str],
    description: str,
) -> Callable[..., Any]:
    step = Step(
        name=fn.__name__,
        fn=fn,
        output_name=output or fn.__name__,
        input_names=_input_names(fn),
        kind=kind,
        fn_source=_fn_source(fn),
        schedule=Schedule.parse(schedule),
        incremental=incremental,
        merge=MergeMode(merge),
        merge_key=list(merge_key) if merge_key else None,
        partition_by=partition_by,
        max_partitions=max_partitions,
        timeout=parse_duration(timeout),
        retry=retry,
        retry_on=retry_on,
        tags=list(tags),
        description=description,
    )
    get_registry().register(step)
    return fn


# --------------------------------------------------------------------------- #
# @step
# --------------------------------------------------------------------------- #

@overload
def step(fn: F) -> F: ...
@overload
def step(
    *,
    output: str | None = None,
    schedule: str | None = None,
    incremental: bool = False,
    merge: str = "replace",
    merge_key: Sequence[str] | None = None,
    partition_by: str | None = None,
    max_partitions: int = 8,
    timeout: str = "15m",
    retry: int = 2,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    tags: list[str] | None = None,
    description: str = "",
) -> Callable[[F], F]: ...


def step(
    fn: F | None = None,
    *,
    output: str | None = None,
    schedule: str | None = None,
    incremental: bool = False,
    merge: str = "replace",
    merge_key: Sequence[str] | None = None,
    partition_by: str | None = None,
    max_partitions: int = 8,
    timeout: str = "15m",
    retry: int = 2,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    tags: list[str] | None = None,
    description: str = "",
) -> Any:
    def decorator(f: F) -> F:
        return _register(  # type: ignore[return-value]
            f,
            kind=StepKind.STEP,
            output=output,
            schedule=schedule,
            incremental=incremental,
            merge=merge,
            merge_key=merge_key,
            partition_by=partition_by,
            max_partitions=max_partitions,
            timeout=timeout,
            retry=retry,
            retry_on=retry_on,
            tags=tags or [],
            description=description,
        )

    return decorator(fn) if fn is not None else decorator


# --------------------------------------------------------------------------- #
# @source
# --------------------------------------------------------------------------- #

@overload
def source(fn: F) -> F: ...
@overload
def source(
    *,
    output: str | None = None,
    schedule: str | None = None,
    timeout: str = "15m",
    retry: int = 2,
    tags: list[str] | None = None,
    description: str = "",
) -> Callable[[F], F]: ...


def source(
    fn: F | None = None,
    *,
    output: str | None = None,
    schedule: str | None = None,
    timeout: str = "15m",
    retry: int = 2,
    tags: list[str] | None = None,
    description: str = "",
) -> Any:
    def decorator(f: F) -> F:
        return _register(  # type: ignore[return-value]
            f,
            kind=StepKind.SOURCE,
            output=output,
            schedule=schedule,
            incremental=False,
            merge="replace",
            merge_key=None,
            partition_by=None,
            max_partitions=1,
            timeout=timeout,
            retry=retry,
            retry_on=(Exception,),
            tags=tags or [],
            description=description,
        )

    return decorator(fn) if fn is not None else decorator


# --------------------------------------------------------------------------- #
# @sink
# --------------------------------------------------------------------------- #

@overload
def sink(fn: F) -> F: ...
@overload
def sink(
    *,
    schedule: str | None = None,
    timeout: str = "15m",
    retry: int = 2,
    tags: list[str] | None = None,
    description: str = "",
) -> Callable[[F], F]: ...


def sink(
    fn: F | None = None,
    *,
    schedule: str | None = None,
    timeout: str = "15m",
    retry: int = 2,
    tags: list[str] | None = None,
    description: str = "",
) -> Any:
    def decorator(f: F) -> F:
        return _register(  # type: ignore[return-value]
            f,
            kind=StepKind.SINK,
            output=None,  # sinks write externally; output_name == fn name (unused in DAG)
            schedule=schedule,
            incremental=False,
            merge="replace",
            merge_key=None,
            partition_by=None,
            max_partitions=1,
            timeout=timeout,
            retry=retry,
            retry_on=(Exception,),
            tags=tags or [],
            description=description,
        )

    return decorator(fn) if fn is not None else decorator

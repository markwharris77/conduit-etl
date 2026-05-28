"""Core dataclasses and the :class:`Schedule` value type.

These types have no dependency on any backend — they are the vocabulary the rest
of the runtime speaks. ``Table`` is the public alias steps annotate their inputs
and outputs with; it is a thin name for a DuckDB relation so that step functions
are plain, unit-testable functions over relations.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeAlias

import duckdb

# A Table is just a DuckDB relation. Steps take and return these directly, which
# keeps them pure functions that can be tested without any runtime present.
Table: TypeAlias = duckdb.DuckDBPyRelation


class StepKind(str, Enum):
    SOURCE = "source"
    STEP = "step"
    SINK = "sink"


class MergeMode(str, Enum):
    REPLACE = "replace"
    APPEND = "append"
    UPSERT = "upsert"


# --------------------------------------------------------------------------- #
# Scheduling
# --------------------------------------------------------------------------- #

_ALIASES: dict[str, str] = {
    "minutely": "* * * * *",
    "hourly": "0 * * * *",
    "daily": "0 0 * * *",
    "weekly": "0 0 * * 0",
    "monthly": "0 0 1 * *",
}

_DURATION_RE = re.compile(r"^\s*(\d+)\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours|d|day|days)\s*$", re.I)
_DURATION_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}


def parse_duration(text: str) -> timedelta:
    """Parse a duration like ``30s``, ``5m``, ``2h``, ``1d`` into a timedelta."""
    match = _DURATION_RE.match(text)
    if not match:
        raise ValueError(f"invalid duration: {text!r}")
    value, unit = int(match.group(1)), match.group(2).lower()
    return timedelta(seconds=value * _DURATION_UNITS[unit])


def _parse_cron_field(field_text: str, low: int, high: int) -> set[int]:
    """Expand one cron field (``*``, ``*/n``, ``a-b``, ``a,b``, ``n``) to a set."""
    values: set[int] = set()
    for part in field_text.split(","):
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = int(step_text)
        else:
            base = part
        if base in ("*", ""):
            start, end = low, high
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(base)
        values.update(v for v in range(start, end + 1) if (v - start) % step == 0)
    return values


@dataclass(frozen=True)
class _CronSpec:
    minute: set[int]
    hour: set[int]
    dom: set[int]
    month: set[int]
    dow: set[int]

    @classmethod
    def parse(cls, expr: str) -> _CronSpec:
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError(f"cron expression must have 5 fields, got {expr!r}")
        m, h, dom, mon, dow = fields
        return cls(
            minute=_parse_cron_field(m, 0, 59),
            hour=_parse_cron_field(h, 0, 23),
            dom=_parse_cron_field(dom, 1, 31),
            month=_parse_cron_field(mon, 1, 12),
            dow={d % 7 for d in _parse_cron_field(dow, 0, 7)},  # 7 == Sunday == 0
        )

    def matches(self, when: datetime) -> bool:
        return (
            when.minute in self.minute
            and when.hour in self.hour
            and when.month in self.month
            and when.day in self.dom
            and (when.isoweekday() % 7) in self.dow
        )


@dataclass(frozen=True)
class Schedule:
    """When a step is due to run.

    Three flavours:
      * ``always`` — due whenever inputs change (the fingerprint gate dedupes).
      * ``interval`` — due once per fixed wall-clock interval.
      * ``cron`` — due on cron ticks (aliases like ``hourly`` expand to cron).
    """

    raw: str | None
    interval: timedelta | None = None
    _cron: _CronSpec | None = None

    @classmethod
    def parse(cls, spec: str | None) -> Schedule:
        if spec is None:
            return cls(raw=None)
        spec = spec.strip()
        if spec.lower() in ("always", ""):
            return cls(raw=None)
        if spec in _ALIASES:
            return cls(raw=spec, _cron=_CronSpec.parse(_ALIASES[spec]))
        if _DURATION_RE.match(spec):
            return cls(raw=spec, interval=parse_duration(spec))
        # Treat anything else as a 5-field cron expression.
        return cls(raw=spec, _cron=_CronSpec.parse(spec))

    @property
    def is_always(self) -> bool:
        return self.raw is None

    def is_due(self, last_run: datetime | None, now: datetime) -> bool:
        if self.is_always:
            return True
        if self.interval is not None:
            return last_run is None or (now - last_run) >= self.interval
        if self._cron is not None:
            if not self._cron.matches(now):
                return False
            if last_run is None:
                return True
            # Fire at most once per matching minute.
            return last_run.replace(second=0, microsecond=0) < now.replace(second=0, microsecond=0)
        return True


# --------------------------------------------------------------------------- #
# Catalog / runtime records
# --------------------------------------------------------------------------- #


@dataclass
class Snapshot:
    """A point-in-time version of a table in the catalog."""

    id: str
    table: str
    created_at: datetime
    rows: int
    schema_hash: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """What an executor returns after running a step — staged, not yet committed."""

    step_name: str
    staging_path: str
    rows: int
    duration_seconds: float
    schema: dict[str, str]


@dataclass
class Job:
    """A unit of work tracked by the queue backend."""

    id: str
    step_name: str
    level: int
    input_snapshots: dict[str, str]  # table -> snapshot_id
    created_at: datetime
    claimed_by: str | None = None
    started_at: datetime | None = None


@dataclass
class RunRecord:
    """One execution of a step, persisted to the catalog's run log."""

    id: str
    step_name: str
    output_table: str
    status: str  # "success" | "failed" | "skipped"
    snapshot_id: str | None
    fingerprint: dict[str, Any]
    rows: int
    duration_seconds: float
    started_at: datetime
    finished_at: datetime
    error: str | None = None


@dataclass
class Step:
    """A registered unit of pipeline work (source, step, or sink)."""

    name: str
    fn: Callable[..., Any]
    output_name: str
    input_names: list[str]
    kind: StepKind
    fn_source: str
    schedule: Schedule = field(default_factory=lambda: Schedule.parse(None))
    incremental: bool = False
    merge: MergeMode = MergeMode.REPLACE
    merge_key: Sequence[str] | None = None
    partition_by: str | None = None
    max_partitions: int = 8
    timeout: timedelta = field(default_factory=lambda: parse_duration("15m"))
    retry: int = 2
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    tags: list[str] = field(default_factory=list)
    description: str = ""

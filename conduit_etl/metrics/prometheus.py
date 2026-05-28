"""Prometheus text format metrics.

Writes raw Prometheus text format — no SDK dependency. The scheduler exposes
these at GET /metrics (Phase 2); in Phase 1 they are written to stdout or a
file via ``conduit status``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import IO


@dataclass
class Counter:
    name: str
    help: str
    _values: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict, init=False)

    def inc(self, labels: dict[str, str] | None = None, amount: float = 1.0) -> None:
        key = tuple(sorted((labels or {}).items()))
        self._values[key] = self._values.get(key, 0.0) + amount

    def render(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} counter"]
        for label_pairs, value in sorted(self._values.items()):
            label_str = _format_labels(dict(label_pairs))
            lines.append(f"{self.name}{label_str} {value:g}")
        return "\n".join(lines)


@dataclass
class Gauge:
    name: str
    help: str
    _values: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict, init=False)

    def set(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = tuple(sorted((labels or {}).items()))
        self._values[key] = value

    def render(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} gauge"]
        for label_pairs, value in sorted(self._values.items()):
            label_str = _format_labels(dict(label_pairs))
            lines.append(f"{self.name}{label_str} {value:g}")
        return "\n".join(lines)


@dataclass
class Histogram:
    name: str
    help: str
    buckets: tuple[float, ...] = (1, 5, 10, 30, 60, 120, 300, 600)
    _observations: dict[tuple[tuple[str, str], ...], list[float]] = field(
        default_factory=dict, init=False
    )

    def observe(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = tuple(sorted((labels or {}).items()))
        self._observations.setdefault(key, []).append(value)

    def render(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} histogram"]
        for label_pairs, observations in sorted(self._observations.items()):
            base_labels = dict(label_pairs)
            total = sum(observations)
            count = len(observations)
            for le in self.buckets:
                bucket_count = sum(1 for v in observations if v <= le)
                label_str = _format_labels({**base_labels, "le": str(le)})
                lines.append(f"{self.name}_bucket{label_str} {bucket_count}")
            lines.append(f'{self.name}_bucket{_format_labels({**base_labels, "le": "+Inf"})} {count}')
            lines.append(f"{self.name}_sum{_format_labels(base_labels)} {total:g}")
            lines.append(f"{self.name}_count{_format_labels(base_labels)} {count}")
        return "\n".join(lines)


def _format_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    pairs = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return "{" + pairs + "}"


class MetricsRegistry:
    """Holds all metrics for one runtime instance."""

    def __init__(self) -> None:
        self.step_runs = Counter(
            "conduit_etl_step_runs_total", "Total step executions"
        )
        self.step_duration = Histogram(
            "conduit_etl_step_duration_seconds", "Step execution duration"
        )
        self.step_rows_out = Gauge(
            "conduit_etl_step_rows_out", "Rows written per step execution"
        )
        self.pipeline_lag = Gauge(
            "conduit_etl_pipeline_lag_seconds", "Seconds behind schedule (key health signal)"
        )
        self.catalog_snapshots = Counter(
            "conduit_etl_catalog_snapshots_total", "Total snapshots in catalog"
        )
        self.queue_depth = Gauge(
            "conduit_etl_queue_depth", "Current jobs waiting in queue"
        )

    def record_step(
        self,
        step_name: str,
        status: str,
        duration_seconds: float,
        rows: int,
    ) -> None:
        labels = {"step": step_name, "status": status}
        self.step_runs.inc(labels)
        self.step_duration.observe(duration_seconds, {"step": step_name})
        if status == "success":
            self.step_rows_out.set(float(rows), {"step": step_name})

    def render(self) -> str:
        metrics = [
            self.step_runs,
            self.step_duration,
            self.step_rows_out,
            self.pipeline_lag,
            self.catalog_snapshots,
            self.queue_depth,
        ]
        return "\n\n".join(m.render() for m in metrics) + "\n"

    def write(self, out: IO[str]) -> None:
        out.write(self.render())

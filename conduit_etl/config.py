"""TOML configuration loader with environment variable expansion.

Loads ``pipeline.toml`` (or a custom path) and expands ``${VAR}`` references
throughout the file. Falls back to sane defaults if a section is missing.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from conduit_etl.core.errors import ConfigError

_ENV_RE = re.compile(r"\$\{([^}]+)}")

_DEFAULT_PATHS = [Path("pipeline.toml"), Path.home() / ".conduit" / "pipeline.toml"]


def _expand(value: object) -> object:
    if isinstance(value, str):
        def replace(m: re.Match) -> str:  # type: ignore[type-arg]
            var = m.group(1)
            expanded = os.environ.get(var)
            if expanded is None:
                raise ConfigError(f"environment variable ${{{var}}} is not set")
            return expanded
        return _ENV_RE.sub(replace, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


@dataclass
class CatalogConfig:
    backend: str = "local"
    path: str = "~/.conduit/catalog"
    url: str = ""
    endpoint: str = ""
    key: str = ""
    secret: str = ""


@dataclass
class QueueConfig:
    backend: str = "memory"
    path: str = "~/.conduit/queue.db"
    url: str = ""


@dataclass
class ExecutorConfig:
    backend: str = "local"
    workers: int = 4
    staging_path: str = "/tmp/conduit/staging"
    scheduler_url: str = ""


@dataclass
class SchedulerConfig:
    port: int = 7700
    metrics_port: int = 7701
    tick: str = "10s"
    heartbeat_window: str = "30s"


@dataclass
class StepsConfig:
    default_timeout: str = "15m"
    default_retry: int = 2
    staging_path: str = "/tmp/conduit/staging"


@dataclass
class MonitoringConfig:
    log_level: str = "info"
    log_format: str = "json"


@dataclass
class PipelineConfig:
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    steps: StepsConfig = field(default_factory=StepsConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    connections: dict[str, dict] = field(default_factory=dict)
    raw: dict = field(default_factory=dict)


def _section(raw: dict, key: str, cls: type, **overrides: object) -> object:
    data = dict(raw.get(key, {}))
    data.update({k: v for k, v in overrides.items() if v})
    valid = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    filtered = {k: v for k, v in data.items() if k in valid}
    return cls(**filtered)


def load(path: str | Path | None = None) -> PipelineConfig:
    """Load and return a PipelineConfig from ``path`` (or the default search path)."""
    resolved: Path | None = None
    if path is not None:
        resolved = Path(path).expanduser()
        if not resolved.exists():
            raise ConfigError(f"config file not found: {resolved}")
    else:
        for candidate in _DEFAULT_PATHS:
            if candidate.exists():
                resolved = candidate
                break

    raw: dict = {}
    if resolved is not None:
        try:
            with resolved.open("rb") as fh:
                raw = tomllib.load(fh)
        except Exception as exc:
            raise ConfigError(f"failed to load config {resolved}: {exc}") from exc
        raw = _expand(raw)  # type: ignore[assignment]

    return PipelineConfig(
        catalog=_section(raw, "catalog", CatalogConfig),  # type: ignore[arg-type]
        queue=_section(raw, "queue", QueueConfig),  # type: ignore[arg-type]
        executor=_section(raw, "executor", ExecutorConfig),  # type: ignore[arg-type]
        scheduler=_section(raw, "scheduler", SchedulerConfig),  # type: ignore[arg-type]
        steps=_section(raw, "steps", StepsConfig),  # type: ignore[arg-type]
        monitoring=_section(raw, "monitoring", MonitoringConfig),  # type: ignore[arg-type]
        connections=raw.get("connections", {}),
        raw=raw,
    )

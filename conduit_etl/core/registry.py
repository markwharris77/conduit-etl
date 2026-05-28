"""Global step registry — thread-safe.

All decorated steps (@source, @step, @sink) register themselves here at import
time. The runtime reads the registry to build the DAG and schedule work.
"""

from __future__ import annotations

import threading
from typing import Iterator

from conduit_etl.core.errors import DuplicateStepError, UnknownStepError
from conduit_etl.core.models import Step


class Registry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._steps: dict[str, Step] = {}

    def register(self, step: Step) -> None:
        with self._lock:
            if step.name in self._steps:
                raise DuplicateStepError(f"step {step.name!r} is already registered")
            self._steps[step.name] = step

    def get(self, name: str) -> Step:
        with self._lock:
            if name not in self._steps:
                raise UnknownStepError(f"no step named {name!r}")
            return self._steps[name]

    def all_steps(self) -> list[Step]:
        with self._lock:
            return list(self._steps.values())

    def clear(self) -> None:
        with self._lock:
            self._steps.clear()

    def __iter__(self) -> Iterator[Step]:
        return iter(self.all_steps())

    def __len__(self) -> int:
        with self._lock:
            return len(self._steps)


# Module-level singleton — import and use directly.
_registry = Registry()


def get_registry() -> Registry:
    return _registry

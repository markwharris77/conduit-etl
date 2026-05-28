"""ExecutorBackend — how steps are run.

The executor is responsible for calling the step function and writing its output
to a staging parquet file. It does NOT commit to the catalog — that serialised
write happens in the runtime after the future resolves.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future

import duckdb

from conduit_etl.core.models import Step, StepResult


class ExecutorBackend(ABC):
    @abstractmethod
    def submit(
        self,
        step: Step,
        input_relations: dict[str, duckdb.DuckDBPyRelation],
    ) -> Future[StepResult]:
        """Execute a step. Returns a Future for the staging result."""

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None: ...

    @property
    @abstractmethod
    def active_count(self) -> int: ...

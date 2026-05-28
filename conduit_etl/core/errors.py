"""Exception hierarchy for conduit-etl.

Everything raised by the runtime derives from :class:`ConduitError` so callers
can catch the whole family with one ``except``.
"""

from __future__ import annotations


class ConduitError(Exception):
    """Base class for all conduit-etl errors."""


class ConfigError(ConduitError):
    """Raised when the pipeline configuration is missing or invalid."""


class RegistryError(ConduitError):
    """Raised for problems registering or resolving steps."""


class DuplicateStepError(RegistryError):
    """Two steps registered under the same name."""


class UnknownStepError(RegistryError):
    """A step was requested by a name that is not registered."""


class DAGError(ConduitError):
    """Base class for DAG construction errors."""


class CycleError(DAGError):
    """The step graph contains a cycle and cannot be levelised."""


class CatalogError(ConduitError):
    """Raised for catalog read/write/transaction failures."""


class SnapshotNotFoundError(CatalogError):
    """A snapshot was requested that does not exist."""


class QueueError(ConduitError):
    """Raised for queue backend failures."""


class ExecutionError(ConduitError):
    """Raised when a step fails to execute.

    Carries the originating step name and the underlying cause so the runtime
    can record a meaningful run record.
    """

    def __init__(self, step_name: str, message: str, cause: BaseException | None = None) -> None:
        super().__init__(f"step {step_name!r} failed: {message}")
        self.step_name = step_name
        self.cause = cause


class StepTimeoutError(ExecutionError):
    """A step exceeded its configured timeout."""


class SourceError(ConduitError):
    """Raised for source connector failures."""


class SinkError(ConduitError):
    """Raised for sink connector failures."""

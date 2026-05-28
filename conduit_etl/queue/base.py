"""QueueBackend — how ready jobs are tracked.

The queue is a hand-off between the scheduler (which decides what is ready) and
the executor (which runs jobs). It is intentionally narrow: claim, heartbeat,
complete, fail. Concrete backends differ in durability and where they live.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from conduit_etl.core.models import Job, Snapshot


class QueueBackend(ABC):
    @abstractmethod
    def enqueue(self, job: Job) -> None: ...

    @abstractmethod
    def claim(self, worker_id: str) -> Job | None:
        """Atomically claim and return one job, or ``None`` if the queue is empty."""

    @abstractmethod
    def heartbeat(self, job_id: str, worker_id: str) -> None: ...

    @abstractmethod
    def complete(self, job_id: str, output_snapshot: Snapshot) -> None: ...

    @abstractmethod
    def fail(self, job_id: str, error: str) -> None: ...

    @abstractmethod
    def requeue_stale(self, heartbeat_window_seconds: int) -> int:
        """Re-queue jobs whose worker has gone silent. Returns count re-queued."""

    @abstractmethod
    def pending_count(self) -> int: ...

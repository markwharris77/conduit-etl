"""DistributedExecutor — submits jobs to the queue; workers execute them.

The submit() call enqueues a job and returns a manually-resolved Future. When a
worker reports completion via the scheduler HTTP API, the server calls
``resolve_done`` or ``resolve_failed`` on this executor, which sets the future
result and unblocks the Runtime's ``as_completed`` loop.
"""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path

import duckdb

from conduit_etl.core.errors import ExecutionError
from conduit_etl.core.models import Job, Step, StepResult
from conduit_etl.executor.base import ExecutorBackend
from conduit_etl.queue.base import QueueBackend


class DistributedExecutor(ExecutorBackend):
    def __init__(self, queue: QueueBackend, staging_path: str = "/tmp/conduit/staging") -> None:
        self._queue = queue
        self._staging = Path(staging_path)
        self._staging.mkdir(parents=True, exist_ok=True)
        self._pending: dict[str, Future[StepResult]] = {}
        self._lock = threading.Lock()
        self._active = 0

    def submit(
        self,
        step: Step,
        input_relations: dict[str, duckdb.DuckDBPyRelation],
        *,
        input_snapshots: dict[str, str] | None = None,
    ) -> Future[StepResult]:
        job_id = uuid.uuid4().hex
        job = Job(
            id=job_id,
            step_name=step.name,
            level=0,  # Runtime sets level; not tracked here
            input_snapshots=input_snapshots or {},
            created_at=datetime.now(),
        )
        fut: Future[StepResult] = Future()
        with self._lock:
            self._pending[job_id] = fut
            self._active += 1
        self._queue.enqueue(job)
        return fut

    def resolve_done(self, job_id: str, result: StepResult) -> None:
        """Called by the HTTP server when a worker reports success."""
        with self._lock:
            fut = self._pending.pop(job_id, None)
            if fut is not None:
                self._active -= 1
        if fut is not None:
            fut.set_result(result)

    def resolve_failed(self, job_id: str, error: str, step_name: str = "unknown") -> None:
        """Called by the HTTP server when a worker reports failure."""
        with self._lock:
            fut = self._pending.pop(job_id, None)
            if fut is not None:
                self._active -= 1
        if fut is not None:
            fut.set_exception(ExecutionError(step_name, error))

    def pending_job_ids(self) -> list[str]:
        with self._lock:
            return list(self._pending)

    def shutdown(self, wait: bool = True) -> None:
        if wait:
            # Cancel any still-pending futures (workers are gone)
            with self._lock:
                for fut in self._pending.values():
                    if not fut.done():
                        fut.cancel()
                self._pending.clear()

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active

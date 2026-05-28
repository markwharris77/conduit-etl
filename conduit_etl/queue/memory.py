"""MemoryQueue — in-process, thread-safe.

The default Phase 1 queue. Loses state on restart, which is fine — the
scheduler reconstructs ready jobs from the catalog on every tick anyway.
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Final

from conduit_etl.core.errors import QueueError
from conduit_etl.core.models import Job, Snapshot
from conduit_etl.queue.base import QueueBackend


class MemoryQueue(QueueBackend):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: deque[Job] = deque()
        self._in_flight: dict[str, Job] = {}
        self._heartbeats: dict[str, datetime] = {}
        self._results: dict[str, Snapshot] = {}
        self._failures: dict[str, str] = {}

    def enqueue(self, job: Job) -> None:
        with self._lock:
            self._pending.append(job)

    def claim(self, worker_id: str) -> Job | None:
        with self._lock:
            if not self._pending:
                return None
            job = self._pending.popleft()
            job.claimed_by = worker_id
            job.started_at = datetime.now()
            self._in_flight[job.id] = job
            self._heartbeats[job.id] = job.started_at
            return job

    def heartbeat(self, job_id: str, worker_id: str) -> None:
        with self._lock:
            job = self._in_flight.get(job_id)
            if job is None:
                raise QueueError(f"heartbeat for unknown job {job_id}")
            if job.claimed_by != worker_id:
                raise QueueError(
                    f"heartbeat from {worker_id!r} for job claimed by {job.claimed_by!r}"
                )
            self._heartbeats[job_id] = datetime.now()

    def complete(self, job_id: str, output_snapshot: Snapshot) -> None:
        with self._lock:
            job = self._in_flight.pop(job_id, None)
            if job is None:
                raise QueueError(f"complete for unknown job {job_id}")
            self._heartbeats.pop(job_id, None)
            self._results[job_id] = output_snapshot

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            job = self._in_flight.pop(job_id, None)
            if job is None:
                raise QueueError(f"fail for unknown job {job_id}")
            self._heartbeats.pop(job_id, None)
            self._failures[job_id] = error

    def requeue_stale(self, heartbeat_window_seconds: int) -> int:
        cutoff = datetime.now() - timedelta(seconds=heartbeat_window_seconds)
        requeued = 0
        with self._lock:
            for job_id, last in list(self._heartbeats.items()):
                if last < cutoff:
                    job = self._in_flight.pop(job_id, None)
                    if job is None:
                        continue
                    self._heartbeats.pop(job_id, None)
                    job.claimed_by = None
                    job.started_at = None
                    self._pending.append(job)
                    requeued += 1
        return requeued

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    # Helpers (not in the abstract interface, but useful for tests/runtime)
    def in_flight_count(self) -> int:
        with self._lock:
            return len(self._in_flight)

    def result_for(self, job_id: str) -> Snapshot | None:
        with self._lock:
            return self._results.get(job_id)


# Sentinel — exported so backends agree on the heartbeat window default if a
# caller doesn't supply one.
DEFAULT_HEARTBEAT_SECONDS: Final[int] = 30

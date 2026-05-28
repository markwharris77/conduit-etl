"""Pipeline runtime — the tick loop that wires everything together.

One ``Runtime`` instance owns a catalog, queue, executor, and registry. Each
``tick()`` call inspects the registry, builds the current DAG, decides which
steps are due, submits ready steps to the executor concurrently, then commits
their results to the catalog serially (preserving the catalog's atomicity contract).

The ``run_once()`` method executes all due steps to completion and returns.
The ``run_forever()`` method loops until interrupted.
"""

from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, wait as futures_wait
from datetime import datetime
from typing import Any

from conduit_etl.catalog.base import CatalogBackend
from conduit_etl.core.dag import execution_order
from conduit_etl.core.errors import ExecutionError
from conduit_etl.core.fingerprint import compute_fingerprint, fingerprint_changed
from conduit_etl.core.models import RunRecord, Step
from conduit_etl.core.registry import Registry
from conduit_etl.executor.base import ExecutorBackend
from conduit_etl.queue.base import QueueBackend

log = logging.getLogger(__name__)


class Runtime:
    def __init__(
        self,
        catalog: CatalogBackend,
        queue: QueueBackend,
        executor: ExecutorBackend,
        registry: Registry,
        *,
        tick_interval: float = 10.0,
        tags: list[str] | None = None,
        step_names: list[str] | None = None,
    ) -> None:
        self.catalog = catalog
        self.queue = queue
        self.executor = executor
        self.registry = registry
        self.tick_interval = tick_interval
        self._filter_tags = set(tags or [])
        self._filter_names = set(step_names or [])

    # ---------------------------------------------------------------------- #
    # Public interface
    # ---------------------------------------------------------------------- #

    def run_once(self) -> dict[str, str]:
        """Run all due steps once. Returns {step_name: status} for every step visited."""
        return self.tick()

    def run_forever(self) -> None:
        log.info("conduit scheduler started (tick=%.1fs)", self.tick_interval)
        try:
            while True:
                self.tick()
                time.sleep(self.tick_interval)
        except KeyboardInterrupt:
            log.info("conduit scheduler stopping")
        finally:
            self.executor.shutdown(wait=True)

    # ---------------------------------------------------------------------- #
    # Core tick
    # ---------------------------------------------------------------------- #

    def tick(self) -> dict[str, str]:
        now = datetime.now()
        steps = self._filtered_steps()
        if not steps:
            return {}

        levels = execution_order(steps)
        results: dict[str, str] = {}

        for level in levels:
            ready = self._ready_steps(level, now)
            if not ready:
                for s in level:
                    results.setdefault(s.name, "skipped")
                continue

            futures = {
                self.executor.submit(step, self._resolve_inputs(step)): (step, fp)
                for step, fp in ready
            }

            pending = set(futures)
            while pending:
                done, pending = futures_wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    step, fp = futures[fut]
                    started_at = datetime.now()
                    try:
                        result = fut.result()
                    except Exception as exc:
                        err = exc if isinstance(exc, ExecutionError) else ExecutionError(step.name, str(exc), exc)
                        log.error("step %r failed: %s", step.name, err)
                        self._record(step, fp, status="failed", error=str(err), started_at=started_at)
                        results[step.name] = "failed"
                        continue

                    # Commit to catalog (serialised — one at a time)
                    try:
                        staged = self.catalog.staged_relation(result.staging_path)
                        with self.catalog.transaction() as txn:
                            meta = {
                                "step": step.name,
                                "merge": step.merge.value,
                                "merge_key": step.merge_key,
                                "input_fingerprint": fp,
                                "rows": result.rows,
                                "duration_seconds": result.duration_seconds,
                            }
                            snap = txn.write(step.output_name, staged, meta)
                            txn.commit()
                        snap.meta.update(meta)
                    except Exception as exc:
                        log.error("catalog commit failed for %r: %s", step.name, exc)
                        self._record(step, fp, status="failed", error=str(exc), started_at=started_at)
                        results[step.name] = "failed"
                        continue

                    self._record(
                        step, fp,
                        status="success",
                        snapshot_id=snap.id,
                        rows=result.rows,
                        duration_seconds=result.duration_seconds,
                        started_at=started_at,
                    )
                    log.info(
                        "step %r succeeded: %d rows in %.2fs",
                        step.name, result.rows, result.duration_seconds,
                    )
                    results[step.name] = "success"

            # Mark non-ready steps in this level as skipped
            ready_names = {s.name for s, _ in ready}
            for s in level:
                if s.name not in ready_names:
                    results.setdefault(s.name, "skipped")

        return results

    # ---------------------------------------------------------------------- #
    # Helpers
    # ---------------------------------------------------------------------- #

    def _filtered_steps(self) -> list[Step]:
        steps = self.registry.all_steps()
        if self._filter_names:
            steps = [s for s in steps if s.name in self._filter_names]
        if self._filter_tags:
            steps = [s for s in steps if self._filter_tags.intersection(s.tags)]
        return steps

    def _ready_steps(self, level: list[Step], now: datetime) -> list[tuple[Step, dict[str, Any]]]:
        ready = []
        for step in level:
            last = self.catalog.last_run(step.name, only_success=True)
            last_run_time = last.finished_at if last else None

            if not step.schedule.is_due(last_run_time, now):
                log.debug("step %r: not due", step.name)
                continue

            fp = compute_fingerprint(step, self.catalog)
            if None in fp.values():
                log.debug("step %r: inputs not yet available", step.name)
                continue

            prev_fp = last.fingerprint if last else None
            if not fingerprint_changed(prev_fp, fp):
                log.debug("step %r: fingerprint unchanged, skipping", step.name)
                continue

            ready.append((step, fp))
        return ready

    def _resolve_inputs(self, step: Step) -> dict:
        inputs = {}
        for name in step.input_names:
            snap = self.catalog.latest_snapshot(name)
            if snap is not None:
                inputs[name] = self.catalog.as_relation(snap)
        return inputs

    def _record(
        self,
        step: Step,
        fp: dict[str, Any],
        *,
        status: str,
        snapshot_id: str | None = None,
        rows: int = 0,
        duration_seconds: float = 0.0,
        error: str | None = None,
        started_at: datetime | None = None,
    ) -> None:
        now = datetime.now()
        record = RunRecord(
            id=uuid.uuid4().hex,
            step_name=step.name,
            output_table=step.output_name,
            status=status,
            snapshot_id=snapshot_id,
            fingerprint=fp,
            rows=rows,
            duration_seconds=duration_seconds,
            started_at=started_at or now,
            finished_at=now,
            error=error,
        )
        self.catalog.record_run(record)

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
from concurrent.futures import FIRST_COMPLETED, Future, wait as futures_wait
from datetime import datetime
from typing import Any

from conduit_etl.catalog.base import CatalogBackend
from conduit_etl.core.dag import execution_order
from conduit_etl.core.errors import ExecutionError
from conduit_etl.core.fingerprint import compute_fingerprint, fingerprint_changed
from conduit_etl.core.models import RunRecord, Step, StepKind
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
        heartbeat_window: float = 30.0,
        tags: list[str] | None = None,
        step_names: list[str] | None = None,
    ) -> None:
        self.catalog = catalog
        self.queue = queue
        self.executor = executor
        self.registry = registry
        self.tick_interval = tick_interval
        self.heartbeat_window = heartbeat_window
        self._filter_tags = set(tags or [])
        self._filter_names = set(step_names or [])

    # ---------------------------------------------------------------------- #
    # Public interface
    # ---------------------------------------------------------------------- #

    def run_once(self) -> dict[str, str]:
        """Run all due steps once. Returns {step_name: status} for every step visited."""
        return self.tick()

    def run_forever(self, on_tick: Any = None) -> None:
        """Loop forever, calling ``on_tick()`` after each tick if provided."""
        log.info("conduit scheduler started (tick=%.1fs)", self.tick_interval)
        try:
            while True:
                self.tick()
                if on_tick is not None:
                    on_tick()
                time.sleep(self.tick_interval)
        except KeyboardInterrupt:
            log.info("conduit scheduler stopping")
        finally:
            self.executor.shutdown(wait=True)

    # ---------------------------------------------------------------------- #
    # Core tick
    # ---------------------------------------------------------------------- #

    def tick(self) -> dict[str, str]:
        # Reclaim jobs whose workers have gone silent before deciding what to run.
        requeued = self.queue.requeue_stale(int(self.heartbeat_window))
        if requeued:
            log.info("requeued %d stale job(s)", requeued)

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

            futures = {}
            for step, fp in ready:
                if step.partition_by:
                    partition_futures = self._submit_partitioned(step, fp)
                    futures.update(partition_futures)
                else:
                    fut = self.executor.submit(
                        step,
                        self._resolve_inputs(step, fp),
                        input_snapshots=_extract_snapshots(fp),
                    )
                    futures[fut] = (step, fp)

            pending = set(futures)
            while pending:
                done, pending = futures_wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    step, fp = futures[fut]
                    started_at = datetime.now()
                    try:
                        result = fut.result()
                    except Exception as exc:
                        import traceback as _tb
                        err = exc if isinstance(exc, ExecutionError) else ExecutionError(step.name, str(exc), exc)
                        log.error("step %r failed: %s", step.name, err)
                        self._record(step, fp, status="failed", error=str(err), started_at=started_at)
                        try:
                            self.catalog.record_dead_letter(
                                step_name=step.name,
                                input_snapshot_ids=_extract_snapshots(fp),
                                error=str(err),
                                traceback=_tb.format_exc(),
                            )
                        except Exception:
                            pass
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

            # Sources have no catalog inputs so their fingerprint only contains
            # __fn_hash__, which never changes between runs. Skip the equality
            # check for sources — the schedule gate is the only gate they need.
            if step.kind is not StepKind.SOURCE:
                prev_fp = last.fingerprint if last else None
                if not fingerprint_changed(prev_fp, fp):
                    log.debug("step %r: fingerprint unchanged, skipping", step.name)
                    continue

            ready.append((step, fp))
        return ready

    def _submit_partitioned(
        self, step: Step, fp: dict[str, Any]
    ) -> dict[Future, tuple[Step, dict]]:
        """Fan-out: one sub-job per unique partition value, up to max_partitions."""
        col = step.partition_by
        inputs = self._resolve_inputs(step, fp)
        if not inputs:
            return {}

        # Collect distinct partition values from the first input relation
        first_rel = next(iter(inputs.values()))
        try:
            values = [r[0] for r in first_rel.aggregate(f"distinct {col} AS v").fetchall()]
        except Exception:
            # Column doesn't exist — fall back to non-partitioned
            fut = self.executor.submit(step, inputs, input_snapshots=_extract_snapshots(fp))
            return {fut: (step, fp)}

        values = values[: step.max_partitions]
        futures: dict[Future, tuple[Step, dict]] = {}
        for val in values:
            partition_inputs = {
                name: rel.filter(f"{col} = '{val}'")
                for name, rel in inputs.items()
            }
            partition_fp = {**fp, "__partition__": str(val)}
            fut = self.executor.submit(step, partition_inputs, input_snapshots=_extract_snapshots(fp))
            futures[fut] = (step, partition_fp)
        return futures

    def _resolve_inputs(self, step: Step, fp: dict[str, Any]) -> dict:
        inputs = {}
        for name in step.input_names:
            snap = self.catalog.latest_snapshot(name)
            if snap is None:
                continue
            if step.incremental:
                last = self.catalog.last_run(step.name, only_success=True)
                if last and last.fingerprint.get(name) is not None:
                    prev_snap_id = last.fingerprint[name][0]
                    try:
                        inputs[name] = self.catalog.new_rows_since(name, prev_snap_id)
                        continue
                    except Exception as exc:
                        log.warning(
                            "step %r: incremental new_rows_since failed for %r "
                            "(falling back to full snapshot): %s",
                            step.name, name, exc,
                        )
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


def _merge_parquets(paths: list[str], staging_dir: str) -> str:
    """Combine multiple parquet files from partition sub-jobs into one."""
    import tempfile
    import duckdb
    from pathlib import Path

    out = Path(staging_dir) / f"merged-{uuid.uuid4().hex}.parquet"
    con = duckdb.connect()
    quoted = ", ".join(f"'{p}'" for p in paths)
    con.execute(f"COPY (SELECT * FROM read_parquet([{quoted}])) TO '{out}' (FORMAT PARQUET)")
    con.close()
    return str(out)


def _extract_snapshots(fp: dict[str, Any]) -> dict[str, str]:
    """Pull table→snapshot_id pairs out of a fingerprint dict."""
    return {
        k: v[0]
        for k, v in fp.items()
        if not k.startswith("__") and isinstance(v, list) and len(v) >= 1
    }

"""LocalExecutor — runs steps in a ThreadPoolExecutor.

Each step call gets its own thread. The step function receives DuckDB relations
as arguments. Results are staged to a parquet file in the configured staging dir
before the future resolves, so the runtime can commit them to the catalog serially.
"""

from __future__ import annotations

import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import duckdb

from conduit_etl.core.errors import ExecutionError
from conduit_etl.core.models import Step, StepResult
from conduit_etl.core.fingerprint import schema_hash
from conduit_etl.executor.base import ExecutorBackend


class LocalExecutor(ExecutorBackend):
    def __init__(self, workers: int = 4, staging_path: str = "/tmp/conduit/staging") -> None:
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="conduit-worker")
        self._staging = Path(staging_path)
        self._staging.mkdir(parents=True, exist_ok=True)
        self._active = 0
        self._lock = __import__("threading").Lock()

    def submit(
        self,
        step: Step,
        input_relations: dict[str, duckdb.DuckDBPyRelation],
        *,
        input_snapshots: dict[str, str] | None = None,
    ) -> Future[StepResult]:
        # Isolate inputs on the calling (main) thread so all catalog reads happen
        # serially — worker threads only ever receive private in-memory copies.
        isolated = self._isolate(input_relations)
        with self._lock:
            self._active += 1
        return self._pool.submit(self._run, step, isolated)

    def _run(self, step: Step, input_relations: dict[str, duckdb.DuckDBPyRelation]) -> StepResult:
        try:
            return self._execute(step, input_relations)
        finally:
            with self._lock:
                self._active -= 1

    def _isolate(
        self, relations: dict[str, duckdb.DuckDBPyRelation]
    ) -> dict[str, duckdb.DuckDBPyRelation]:
        """Materialise each input relation into a private in-memory DuckDB connection.

        Called on the main thread before submitting to the thread pool, so catalog
        reads are serialised. Each worker gets a fully independent copy — safe for
        concurrent execution with no shared DuckDB connection.
        """
        if not relations:
            return {}
        isolated: dict[str, duckdb.DuckDBPyRelation] = {}
        con = duckdb.connect()
        for name, rel in relations.items():
            tmp = self._staging / f"_iso-{uuid.uuid4().hex}.parquet"
            try:
                rel.write_parquet(str(tmp))
                safe = name.replace("-", "_")
                con.execute(
                    f'CREATE TABLE "{safe}" AS SELECT * FROM read_parquet(\'{tmp}\')'
                )
                isolated[name] = con.table(safe)
            except Exception:
                isolated[name] = rel  # fall back to the original on error
            finally:
                tmp.unlink(missing_ok=True)
        return isolated

    def _execute(
        self, step: Step, input_relations: dict[str, duckdb.DuckDBPyRelation]
    ) -> StepResult:
        start = time.monotonic()
        kwargs = {
            name: input_relations[name]
            for name in step.input_names
            if name in input_relations
        }

        try:
            result = step.fn(**kwargs)
        except Exception as exc:
            raise ExecutionError(step.name, str(exc), cause=exc) from exc

        if result is None:
            raise ExecutionError(step.name, "step returned None — expected a DuckDB relation")

        staging_file = self._staging / f"{step.name}-{uuid.uuid4().hex}.parquet"
        try:
            result.write_parquet(str(staging_file))
        except Exception as exc:
            raise ExecutionError(
                step.name, f"failed to write staging parquet: {exc}", cause=exc
            ) from exc

        rows = int(result.aggregate("count(*) AS n").fetchone()[0])
        sh = schema_hash(result)
        duration = time.monotonic() - start

        return StepResult(
            step_name=step.name,
            staging_path=str(staging_file),
            rows=rows,
            duration_seconds=duration,
            schema={col: str(t) for col, t in zip(result.columns, result.types)},
        )

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active

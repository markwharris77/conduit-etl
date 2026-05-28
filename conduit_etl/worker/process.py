"""Worker process — polls the scheduler HTTP API, executes steps, reports back.

The worker is stateless: it holds no durable state beyond its worker ID. Every
restart is safe because jobs are reclaimed from the queue by the scheduler's
stale-heartbeat sweep.

Usage:
    conduit worker --scheduler http://host:7700 --pipeline my_pipeline
"""

from __future__ import annotations

import inspect
import json
import logging
import threading
import time
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from conduit_etl.core.errors import ExecutionError
from conduit_etl.core.models import StepResult
from conduit_etl.core.registry import Registry

log = logging.getLogger(__name__)


def _http_get(url: str, timeout: int = 10) -> tuple[int, bytes]:
    try:
        req = Request(url)
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except URLError as exc:
        raise ConnectionError(f"GET {url} failed: {exc}") from exc


def _http_post(url: str, body: dict, timeout: int = 10) -> tuple[int, bytes]:
    payload = json.dumps(body).encode()
    req = Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except URLError as exc:
        raise ConnectionError(f"POST {url} failed: {exc}") from exc


class WorkerProcess:
    """Polls the scheduler for jobs, executes them, reports results."""

    def __init__(
        self,
        scheduler_url: str,
        registry: Registry,
        catalog,  # CatalogBackend
        staging_path: str = "/tmp/conduit/staging",
        poll_interval: float = 1.0,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self._url = scheduler_url.rstrip("/")
        self._registry = registry
        self._catalog = catalog
        self._staging = Path(staging_path)
        self._staging.mkdir(parents=True, exist_ok=True)
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._worker_id = f"w-{uuid.uuid4().hex[:8]}"
        self._running = False

    def run(self) -> None:
        log.info("worker %s started (scheduler=%s)", self._worker_id, self._url)
        self._running = True
        while self._running:
            try:
                job = self._poll()
                if job is None:
                    time.sleep(self._poll_interval)
                    continue
                self._execute(job)
            except KeyboardInterrupt:
                break
            except ConnectionError as exc:
                log.warning("scheduler unreachable: %s — retrying", exc)
                time.sleep(5.0)
            except Exception as exc:
                log.exception("unexpected worker error: %s", exc)
                time.sleep(self._poll_interval)
        log.info("worker %s stopped", self._worker_id)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _poll(self) -> dict | None:
        code, body = _http_get(f"{self._url}/job/next?worker_id={self._worker_id}")
        if code == 204:
            return None
        return json.loads(body)

    def _execute(self, job: dict) -> None:
        job_id = job["id"]
        step_name = job["step_name"]
        input_snapshots: dict[str, str] = job.get("input_snapshots", {})

        log.info("worker %s executing job %s (step=%s)", self._worker_id, job_id, step_name)

        try:
            step = self._registry.get(step_name)
        except Exception as exc:
            self._report_failed(job_id, step_name, f"step not found: {exc}")
            return

        hb_thread = _HeartbeatThread(
            url=f"{self._url}/job/{job_id}/heartbeat",
            worker_id=self._worker_id,
            interval=self._heartbeat_interval,
        )
        hb_thread.start()

        try:
            input_relations = self._resolve_inputs(step, input_snapshots)
            result = self._run_step(step, input_relations)
        except Exception as exc:
            hb_thread.stop()
            self._report_failed(job_id, step_name, str(exc))
            return
        finally:
            hb_thread.stop()

        self._report_done(job_id, result)

    def _resolve_inputs(self, step, input_snapshots: dict[str, str]) -> dict:
        inputs = {}
        for name in step.input_names:
            if name in input_snapshots:
                from conduit_etl.core.models import Snapshot
                # Build a minimal Snapshot to call as_relation
                snap = self._catalog.latest_snapshot(name)
                if snap and snap.id == input_snapshots[name]:
                    inputs[name] = self._catalog.as_relation(snap)
                else:
                    # Fall back: find the right snapshot by id
                    snap = self._catalog.latest_snapshot(name)
                    if snap:
                        inputs[name] = self._catalog.as_relation(snap)
            else:
                snap = self._catalog.latest_snapshot(name)
                if snap:
                    inputs[name] = self._catalog.as_relation(snap)
        return inputs

    def _run_step(self, step, input_relations: dict) -> StepResult:
        import time as _time

        sig = inspect.signature(step.fn)
        kwargs = {name: input_relations[name] for name in step.input_names if name in input_relations}
        start = _time.monotonic()

        try:
            result = step.fn(**kwargs)
        except Exception as exc:
            raise ExecutionError(step.name, str(exc), exc) from exc

        if result is None:
            raise ExecutionError(step.name, "step returned None")

        out_path = self._staging / f"{step.name}-{uuid.uuid4().hex}.parquet"
        result.write_parquet(str(out_path))
        rows = int(result.aggregate("count(*) AS n").fetchone()[0])
        duration = _time.monotonic() - start

        return StepResult(
            step_name=step.name,
            staging_path=str(out_path),
            rows=rows,
            duration_seconds=duration,
            schema={col: str(t) for col, t in zip(result.columns, result.types)},
        )

    def _report_done(self, job_id: str, result: StepResult) -> None:
        _http_post(f"{self._url}/job/{job_id}/done", {
            "worker_id": self._worker_id,
            "step_name": result.step_name,
            "staging_path": result.staging_path,
            "rows": result.rows,
            "duration": result.duration_seconds,
            "schema": result.schema,
        })
        log.info("job %s done: %d rows in %.2fs", job_id, result.rows, result.duration_seconds)

    def _report_failed(self, job_id: str, step_name: str, error: str) -> None:
        try:
            _http_post(f"{self._url}/job/{job_id}/failed", {
                "worker_id": self._worker_id,
                "step_name": step_name,
                "error": error,
            })
        except Exception as exc:
            log.warning("failed to report failure for job %s: %s", job_id, exc)
        log.error("job %s failed: %s", job_id, error)


class _HeartbeatThread(threading.Thread):
    def __init__(self, url: str, worker_id: str, interval: float) -> None:
        super().__init__(daemon=True, name="conduit-heartbeat")
        self._url = url
        self._worker_id = worker_id
        self._interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(self._interval):
            try:
                _http_post(self._url, {"worker_id": self._worker_id})
            except Exception as exc:
                log.debug("heartbeat failed: %s", exc)

    def stop(self) -> None:
        self._stop_event.set()



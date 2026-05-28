"""Scheduler HTTP server — job coordination API + metrics/health endpoints.

Runs in a background thread alongside the scheduler's tick loop. All state is
shared via the queue and executor references passed at construction time.

Four job endpoints (worker-facing):
  GET  /job/next?worker_id=w1        Claim and return next job, or 204
  POST /job/{id}/heartbeat           Keep-alive from worker
  POST /job/{id}/done                Worker reports success
  POST /job/{id}/failed              Worker reports failure

Two observability endpoints:
  GET  /health                       Liveness check
  GET  /metrics                      Prometheus text format
"""

from __future__ import annotations

import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

from conduit_etl.core.models import StepResult

if TYPE_CHECKING:
    from conduit_etl.executor.distributed import DistributedExecutor
    from conduit_etl.metrics.prometheus import MetricsRegistry
    from conduit_etl.queue.base import QueueBackend

log = logging.getLogger(__name__)

_JOB_RE = re.compile(r"^/job/([^/]+)/(\w+)$")


class _Handler(BaseHTTPRequestHandler):
    """HTTP handler. ``_sched`` is injected as a class attribute by SchedulerServer.start().

    We cannot use ``self.server`` because Python's HTTP machinery overwrites it
    with the TCP server instance. A custom class attribute survives intact.
    """

    _sched: SchedulerServer  # class-level injection — not overridden by __init__

    def log_message(self, fmt: str, *args: object) -> None:  # silence default access log
        pass

    # ------------------------------------------------------------------ #
    # Routing
    # ------------------------------------------------------------------ #

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/health":
            self._respond(200, self._sched.handle_health())
        elif path == "/metrics":
            body = self._sched.handle_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body.encode())))
            self.end_headers()
            self.wfile.write(body.encode())
        elif path == "/job/next":
            worker_id = (qs.get("worker_id") or ["unknown"])[0]
            code, body = self._sched.handle_job_next(worker_id)
            if code == 204:
                self.send_response(204)
                self.end_headers()
            else:
                self._respond(code, body)
        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        m = _JOB_RE.match(parsed.path)
        if m is None:
            self._respond(404, {"error": "not found"})
            return

        job_id, action = m.group(1), m.group(2)
        length = int(self.headers.get("Content-Length", 0))
        body_raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(body_raw)
        except json.JSONDecodeError:
            self._respond(400, {"error": "invalid JSON"})
            return

        if action == "heartbeat":
            code, resp = self._sched.handle_heartbeat(job_id, body)
        elif action == "done":
            code, resp = self._sched.handle_done(job_id, body)
        elif action == "failed":
            code, resp = self._sched.handle_failed(job_id, body)
        else:
            code, resp = 404, {"error": "unknown action"}

        self._respond(code, resp)

    def _respond(self, code: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class SchedulerServer:
    """Wraps a ThreadingHTTPServer and owns job/metrics state.

    Instantiate, then call ``start()`` to launch the server in a daemon thread.
    Call ``stop()`` to shut it down.
    """

    def __init__(
        self,
        queue: QueueBackend,
        executor: DistributedExecutor,
        metrics: MetricsRegistry,
    ) -> None:
        self._queue = queue
        self._executor = executor
        self._metrics = metrics
        self._tick_count = 0
        self._active_workers: set[str] = set()
        self._lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self, host: str = "0.0.0.0", port: int = 7700) -> None:
        # Inject self as a class attribute so the handler can reach SchedulerServer
        # without being overridden by Python's HTTP server machinery.
        handler_cls = type("_BoundHandler", (_Handler,), {"_sched": self})
        httpd = ThreadingHTTPServer((host, port), handler_cls)
        self._httpd = httpd
        t = threading.Thread(target=httpd.serve_forever, daemon=True, name="conduit-http")
        t.start()
        log.info("scheduler HTTP server listening on %s:%d", host, port)

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()

    def increment_tick(self) -> None:
        with self._lock:
            self._tick_count += 1

    # ------------------------------------------------------------------ #
    # Handlers
    # ------------------------------------------------------------------ #

    def handle_health(self) -> dict:
        return {
            "status": "ok",
            "workers": len(self._active_workers),
            "queue_depth": self._queue.pending_count(),
            "tick_count": self._tick_count,
        }

    def handle_metrics(self) -> str:
        self._metrics.queue_depth.set(float(self._queue.pending_count()))
        return self._metrics.render()

    def handle_job_next(self, worker_id: str) -> tuple[int, dict]:
        with self._lock:
            self._active_workers.add(worker_id)
        job = self._queue.claim(worker_id)
        if job is None:
            return 204, {}
        log.debug("job %s claimed by worker %s", job.id, worker_id)
        return 200, {
            "id": job.id,
            "step_name": job.step_name,
            "level": job.level,
            "input_snapshots": job.input_snapshots,
            "created_at": job.created_at.isoformat(),
        }

    def handle_heartbeat(self, job_id: str, body: dict) -> tuple[int, dict]:
        worker_id = body.get("worker_id", "unknown")
        try:
            self._queue.heartbeat(job_id, worker_id)
            return 200, {"ok": True}
        except Exception as exc:
            return 400, {"error": str(exc)}

    def handle_done(self, job_id: str, body: dict) -> tuple[int, dict]:
        worker_id = body.get("worker_id", "unknown")
        staging_path = body.get("staging_path", "")
        rows = int(body.get("rows", 0))
        duration = float(body.get("duration", 0.0))
        step_name = body.get("step_name", "unknown")
        schema = body.get("schema", {})

        result = StepResult(
            step_name=step_name,
            staging_path=staging_path,
            rows=rows,
            duration_seconds=duration,
            schema=schema,
        )
        self._executor.resolve_done(job_id, result)
        log.debug("job %s done (worker=%s, rows=%d)", job_id, worker_id, rows)
        return 200, {"ok": True}

    def handle_failed(self, job_id: str, body: dict) -> tuple[int, dict]:
        worker_id = body.get("worker_id", "unknown")
        error = body.get("error", "unknown error")
        step_name = body.get("step_name", "unknown")
        self._queue.fail(job_id, error)
        self._executor.resolve_failed(job_id, error, step_name)
        log.debug("job %s failed (worker=%s): %s", job_id, worker_id, error)
        return 200, {"ok": True}

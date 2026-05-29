"""Lightweight metrics/health HTTP server — always-on for any executor type.

Serves:
  GET /health   → {"status":"ok","queue_depth":N,"tick_count":N}
  GET /metrics  → Prometheus text format

Used by ``conduit scheduler`` regardless of executor backend. When the
distributed executor is also active, the job-coordination API runs on a
separate port (see ``worker/server.py``).
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from conduit_etl.executor.base import ExecutorBackend
    from conduit_etl.metrics.prometheus import MetricsRegistry
    from conduit_etl.queue.base import QueueBackend

log = logging.getLogger(__name__)


class _MetricsHandler(BaseHTTPRequestHandler):
    _sched: MetricsServer

    def log_message(self, fmt: str, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        if self.path == "/health":
            body = self._sched.handle_health().encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            body = self._sched.handle_metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


class MetricsServer:
    """Minimal HTTP server exposing /health and /metrics on ``metrics_port``."""

    def __init__(
        self,
        queue: QueueBackend,
        metrics: MetricsRegistry,
        executor: ExecutorBackend | None = None,
    ) -> None:
        self._queue = queue
        self._metrics = metrics
        self._executor = executor
        self._tick_count = 0
        self._lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None

    def start(self, host: str = "0.0.0.0", port: int = 7701) -> None:
        handler_cls = type("_BoundMetrics", (_MetricsHandler,), {"_sched": self})
        self._httpd = ThreadingHTTPServer((host, port), handler_cls)
        t = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="conduit-metrics")
        t.start()
        log.info("metrics server listening on %s:%d", host, port)

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()

    def increment_tick(self) -> None:
        with self._lock:
            self._tick_count += 1

    def handle_health(self) -> str:
        import json
        workers = self._executor.active_count if self._executor is not None else 0
        return json.dumps({
            "status": "ok",
            "workers": workers,
            "queue_depth": self._queue.pending_count(),
            "tick_count": self._tick_count,
        })

    def handle_metrics(self) -> str:
        workers = self._executor.active_count if self._executor is not None else 0
        self._metrics.queue_depth.set(float(self._queue.pending_count()))
        self._metrics.worker_active.set(float(workers))
        return self._metrics.render()

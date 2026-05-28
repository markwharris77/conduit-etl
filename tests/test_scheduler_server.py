"""Tests for the scheduler HTTP server."""

import json
import threading
import time
import urllib.request
import pytest
from datetime import datetime

from conduit_etl.core.models import Job, StepResult
from conduit_etl.executor.distributed import DistributedExecutor
from conduit_etl.metrics.prometheus import MetricsRegistry
from conduit_etl.queue.memory import MemoryQueue
from conduit_etl.worker.server import SchedulerServer


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def srv():
    queue = MemoryQueue()
    executor = DistributedExecutor(queue=queue, staging_path="/tmp/conduit-srv-test")
    metrics = MetricsRegistry()
    server = SchedulerServer(queue=queue, executor=executor, metrics=metrics)
    port = _free_port()
    server.start(host="127.0.0.1", port=port)
    server._test_port = port
    server._queue = queue
    server._executor = executor
    time.sleep(0.05)  # let server thread start
    yield server
    server.stop()


def _get(srv, path: str) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{srv._test_port}{path}"
    with urllib.request.urlopen(url) as resp:
        return resp.status, json.loads(resp.read())


def _post(srv, path: str, body: dict) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{srv._test_port}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read())


def test_health(srv):
    code, body = _get(srv, "/health")
    assert code == 200
    assert body["status"] == "ok"
    assert "queue_depth" in body


def test_metrics(srv):
    url = f"http://127.0.0.1:{srv._test_port}/metrics"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200
        content = resp.read().decode()
    assert "conduit_etl_queue_depth" in content


def test_job_next_empty(srv):
    url = f"http://127.0.0.1:{srv._test_port}/job/next?worker_id=w1"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 204


def test_job_next_claim(srv):
    job = Job(id="j1", step_name="s", level=0, input_snapshots={}, created_at=datetime.now())
    srv._queue.enqueue(job)
    code, body = _get(srv, "/job/next?worker_id=w1")
    assert code == 200
    assert body["id"] == "j1"
    assert body["step_name"] == "s"


def test_heartbeat(srv):
    job = Job(id="j2", step_name="s", level=0, input_snapshots={}, created_at=datetime.now())
    srv._queue.enqueue(job)
    _get(srv, "/job/next?worker_id=w1")
    code, body = _post(srv, "/job/j2/heartbeat", {"worker_id": "w1"})
    assert code == 200


def test_done_resolves_future(srv):
    from conduit_etl.core.models import Schedule, Step, StepKind

    step = Step(
        name="s", fn=lambda: None, output_name="s", input_names=[],
        kind=StepKind.STEP, fn_source="", schedule=Schedule.parse(None),
    )
    fut = srv._executor.submit(step, {})
    job = srv._queue.claim("w1")

    code, body = _post(srv, f"/job/{job.id}/done", {
        "worker_id": "w1",
        "step_name": "s",
        "staging_path": "/tmp/x.parquet",
        "rows": 42,
        "duration": 1.5,
        "schema": {},
    })
    assert code == 200
    assert fut.done()
    assert fut.result().rows == 42


def test_failed_resolves_future(srv):
    from conduit_etl.core.models import Schedule, Step, StepKind

    step = Step(
        name="s2", fn=lambda: None, output_name="s2", input_names=[],
        kind=StepKind.STEP, fn_source="", schedule=Schedule.parse(None),
    )
    fut = srv._executor.submit(step, {})
    job = srv._queue.claim("w1")

    code, _ = _post(srv, f"/job/{job.id}/failed", {
        "worker_id": "w1",
        "step_name": "s2",
        "error": "boom",
    })
    assert code == 200
    assert fut.done()
    with pytest.raises(Exception, match="boom"):
        fut.result()

"""Tests for DistributedExecutor — future resolution by external calls."""

import duckdb
import pytest
from datetime import datetime

from conduit_etl.core.models import Job, Schedule, Step, StepKind, StepResult
from conduit_etl.executor.distributed import DistributedExecutor
from conduit_etl.queue.memory import MemoryQueue


def _step(name: str = "s") -> Step:
    return Step(
        name=name,
        fn=lambda: None,
        output_name=name,
        input_names=[],
        kind=StepKind.STEP,
        fn_source="",
        schedule=Schedule.parse(None),
    )


@pytest.fixture
def queue():
    return MemoryQueue()


@pytest.fixture
def executor(queue):
    return DistributedExecutor(queue=queue, staging_path="/tmp/conduit-test-dist")


def test_submit_enqueues_job(executor, queue):
    step = _step()
    fut = executor.submit(step, {})
    assert not fut.done()
    assert queue.pending_count() == 1


def test_resolve_done_sets_future(executor, queue):
    step = _step()
    fut = executor.submit(step, {})
    job = queue.claim("w1")
    result = StepResult(
        step_name="s", staging_path="/tmp/x.parquet", rows=5, duration_seconds=0.1, schema={}
    )
    executor.resolve_done(job.id, result)
    assert fut.done()
    assert fut.result().rows == 5


def test_resolve_failed_sets_exception(executor, queue):
    step = _step()
    fut = executor.submit(step, {})
    job = queue.claim("w1")
    executor.resolve_failed(job.id, "it broke", "s")
    assert fut.done()
    with pytest.raises(Exception, match="it broke"):
        fut.result()


def test_active_count(executor, queue):
    step = _step()
    assert executor.active_count == 0
    fut = executor.submit(step, {})
    assert executor.active_count == 1
    job = queue.claim("w1")
    result = StepResult("s", "/tmp/x.parquet", 1, 0.1, {})
    executor.resolve_done(job.id, result)
    assert executor.active_count == 0


def test_input_snapshots_stored_in_job(executor, queue):
    step = _step()
    executor.submit(step, {}, input_snapshots={"orders": "snap-42"})
    job = queue.claim("w1")
    assert job.input_snapshots == {"orders": "snap-42"}

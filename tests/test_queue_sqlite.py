"""Tests for SQLiteQueue."""

import pytest
from datetime import datetime

from conduit_etl.core.models import Job, Snapshot
from conduit_etl.queue.sqlite import SQLiteQueue


@pytest.fixture
def queue(tmp_path):
    return SQLiteQueue(tmp_path / "queue.db")


def _job(name: str = "my_step", level: int = 0) -> Job:
    return Job(
        id=f"job-{name}",
        step_name=name,
        level=level,
        input_snapshots={},
        created_at=datetime.now(),
    )


def test_enqueue_and_claim(queue):
    queue.enqueue(_job())
    job = queue.claim("w1")
    assert job is not None
    assert job.step_name == "my_step"
    assert job.claimed_by == "w1"


def test_claim_empty_returns_none(queue):
    assert queue.claim("w1") is None


def test_claim_is_exclusive(queue):
    queue.enqueue(_job())
    j1 = queue.claim("w1")
    j2 = queue.claim("w2")
    assert j1 is not None
    assert j2 is None  # already claimed


def test_pending_count(queue):
    queue.enqueue(_job("a"))
    queue.enqueue(_job("b"))
    assert queue.pending_count() == 2
    queue.claim("w1")
    assert queue.pending_count() == 1


def test_heartbeat(queue):
    queue.enqueue(_job())
    job = queue.claim("w1")
    queue.heartbeat(job.id, "w1")  # should not raise


def test_complete(queue):
    queue.enqueue(_job())
    job = queue.claim("w1")
    snap = Snapshot(id="1", table="t", created_at=datetime.now(), rows=10, schema_hash="abc")
    queue.complete(job.id, snap)  # should not raise


def test_fail(queue):
    queue.enqueue(_job())
    job = queue.claim("w1")
    queue.fail(job.id, "something went wrong")  # should not raise


def test_requeue_stale(queue):
    import time
    queue.enqueue(_job())
    job = queue.claim("w1")
    assert job is not None
    # Requeue with tiny window (0 seconds → all in-flight are stale)
    requeued = queue.requeue_stale(heartbeat_window_seconds=0)
    assert requeued == 1
    assert queue.pending_count() == 1


def test_level_ordering(queue):
    queue.enqueue(_job("b", level=1))
    queue.enqueue(_job("a", level=0))
    first = queue.claim("w1")
    assert first.step_name == "a"  # lower level claimed first


def test_idempotent_enqueue(queue):
    j = _job()
    queue.enqueue(j)
    queue.enqueue(j)  # duplicate — should be ignored
    assert queue.pending_count() == 1

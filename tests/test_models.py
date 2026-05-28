"""Tests for models — Schedule, parse_duration, Step construction."""

import pytest
from datetime import datetime, timedelta

from conduit_etl.core.models import Schedule, parse_duration


def test_parse_duration_seconds():
    assert parse_duration("30s") == timedelta(seconds=30)
    assert parse_duration("30 seconds") == timedelta(seconds=30)


def test_parse_duration_minutes():
    assert parse_duration("5m") == timedelta(minutes=5)
    assert parse_duration("5min") == timedelta(minutes=5)


def test_parse_duration_hours():
    assert parse_duration("2h") == timedelta(hours=2)


def test_parse_duration_days():
    assert parse_duration("1d") == timedelta(days=1)


def test_parse_duration_invalid():
    with pytest.raises(ValueError, match="invalid duration"):
        parse_duration("5weeks")


def test_schedule_always():
    s = Schedule.parse(None)
    assert s.is_always
    assert s.is_due(None, datetime.now())
    assert s.is_due(datetime.now(), datetime.now())


def test_schedule_interval_due():
    s = Schedule.parse("1h")
    now = datetime(2024, 1, 1, 12, 0, 0)
    last = datetime(2024, 1, 1, 10, 0, 0)  # 2 hours ago
    assert s.is_due(last, now)


def test_schedule_interval_not_due():
    s = Schedule.parse("1h")
    now = datetime(2024, 1, 1, 12, 0, 0)
    last = datetime(2024, 1, 1, 11, 30, 0)  # 30 min ago
    assert not s.is_due(last, now)


def test_schedule_interval_no_last_run():
    s = Schedule.parse("1h")
    assert s.is_due(None, datetime.now())


def test_schedule_cron_alias_hourly():
    s = Schedule.parse("hourly")
    # Matches on the hour
    assert s.is_due(None, datetime(2024, 1, 1, 12, 0, 0))
    # Does not match mid-hour
    assert not s.is_due(None, datetime(2024, 1, 1, 12, 30, 0))


def test_schedule_cron_fires_once_per_minute():
    s = Schedule.parse("* * * * *")
    now = datetime(2024, 1, 1, 12, 5, 30)
    # Already ran this minute
    last = datetime(2024, 1, 1, 12, 5, 0)
    assert not s.is_due(last, now)
    # Ran last minute
    last_prev = datetime(2024, 1, 1, 12, 4, 59)
    assert s.is_due(last_prev, now)

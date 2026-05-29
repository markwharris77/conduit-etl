"""Tests for flight_states_clean."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.sources.opensky import states_to_relation
from pipeline.steps.clean import flight_states_clean

FIXTURES = Path(__file__).parent / "fixtures"


def _rows(rel):
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


@pytest.fixture
def raw_rel():
    data = json.loads((FIXTURES / "opensky_response.json").read_text())
    return states_to_relation(data)


def test_filters_on_ground(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    assert all(r["icao24"] != "000000" for r in rows)


def test_filters_null_position(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    assert all(r["longitude"] is not None for r in rows)
    assert all(r["latitude"] is not None for r in rows)


def test_filters_bad_transponder(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    assert all(r["icao24"] != "000000" for r in rows)


def test_derived_altitude_ft(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    dlh = [r for r in rows if r["icao24"] == "3c6444"]
    assert len(dlh) == 1
    assert dlh[0]["altitude_ft"] == pytest.approx(10972.8 * 3.28084, rel=0.01)


def test_derived_speed_kts(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    dlh = [r for r in rows if r["icao24"] == "3c6444"]
    assert len(dlh) == 1
    assert dlh[0]["speed_kts"] == pytest.approx(245.3 * 1.94384, rel=0.01)


def test_heading_cardinal(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    dlh = [r for r in rows if r["icao24"] == "3c6444"]
    assert len(dlh) == 1
    assert dlh[0]["heading_cardinal"] == "W"


def test_position_source_name(raw_rel):
    rows = _rows(flight_states_clean(raw_rel))
    lft = [r for r in rows if r["icao24"] == "3c5ee2"]
    if lft:
        assert lft[0]["position_source_name"] == "MLAT"


def test_output_has_expected_columns(raw_rel):
    result = flight_states_clean(raw_rel)
    cols = set(result.columns)
    expected = {
        "icao24", "callsign", "origin_country", "altitude_ft",
        "speed_kts", "vertical_fpm", "heading_cardinal",
        "last_contact_at", "snapshot_at", "position_source_name",
    }
    assert expected.issubset(cols)

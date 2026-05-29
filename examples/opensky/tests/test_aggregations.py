"""Tests for airspace_snapshot, country_stats, airport_traffic, busy_corridors."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.sources.opensky import states_to_relation
from pipeline.sources.airports import csv_to_relation
from pipeline.steps.clean import flight_states_clean
from pipeline.steps.enrich import flight_states_enriched
from pipeline.steps.snapshot import airspace_snapshot
from pipeline.steps.country_stats import country_stats
from pipeline.steps.airport_traffic import airport_traffic
from pipeline.steps.corridors import busy_corridors

FIXTURES = Path(__file__).parent / "fixtures"


def _rows(rel):
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


@pytest.fixture
def enriched_rel():
    data = json.loads((FIXTURES / "opensky_response.json").read_text())
    csv_text = (FIXTURES / "airports_sample.csv").read_text()
    raw = states_to_relation(data)
    clean = flight_states_clean(raw)
    airports = csv_to_relation(csv_text)
    return flight_states_enriched(clean, airports)


def test_airspace_snapshot_one_row_per_aircraft(enriched_rel):
    rows = _rows(airspace_snapshot(enriched_rel))
    icao24s = [r["icao24"] for r in rows]
    assert len(icao24s) == len(set(icao24s)), "one row per icao24"


def test_airspace_snapshot_has_flight_phase(enriched_rel):
    result = airspace_snapshot(enriched_rel)
    assert "flight_phase" in result.columns
    rows = _rows(result)
    assert all(r["flight_phase"] is not None for r in rows)


def test_country_stats_has_counts(enriched_rel):
    rows = _rows(country_stats(enriched_rel))
    assert len(rows) > 0
    assert all(r["aircraft_count"] > 0 for r in rows)


def test_country_stats_columns(enriched_rel):
    result = country_stats(enriched_rel)
    cols = set(result.columns)
    assert {"origin_country", "window_start", "flight_phase",
            "aircraft_count", "avg_altitude_ft", "avg_speed_kts"}.issubset(cols)


def test_airport_traffic_columns(enriched_rel):
    result = airport_traffic(enriched_rel)
    cols = set(result.columns)
    assert {"nearest_airport_icao", "window_start", "flight_phase",
            "aircraft_count", "min_altitude_ft"}.issubset(cols)


def test_busy_corridors_grid_resolution(enriched_rel):
    rows = _rows(busy_corridors(enriched_rel))
    assert len(rows) > 0
    for r in rows:
        assert (r["grid_lat"] * 2) % 1 == pytest.approx(0, abs=1e-9)
        assert (r["grid_lon"] * 2) % 1 == pytest.approx(0, abs=1e-9)


def test_busy_corridors_columns(enriched_rel):
    result = busy_corridors(enriched_rel)
    cols = set(result.columns)
    assert {"grid_lat", "grid_lon", "window_start", "aircraft_count",
            "avg_altitude_ft", "dominant_phase"}.issubset(cols)

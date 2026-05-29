"""Tests for flight_states_enriched."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.sources.opensky import states_to_relation
from pipeline.sources.airports import csv_to_relation
from pipeline.steps.clean import flight_states_clean
from pipeline.steps.enrich import flight_states_enriched

FIXTURES = Path(__file__).parent / "fixtures"


def _rows(rel):
    cols = rel.columns
    return [dict(zip(cols, r)) for r in rel.fetchall()]


@pytest.fixture
def clean_rel():
    data = json.loads((FIXTURES / "opensky_response.json").read_text())
    raw = states_to_relation(data)
    return flight_states_clean(raw)


@pytest.fixture
def airports_rel():
    csv_text = (FIXTURES / "airports_sample.csv").read_text()
    return csv_to_relation(csv_text)


def test_heathrow_match(clean_rel, airports_rel):
    """BAW456 is near Heathrow (51.47, -0.45) — should match EGLL."""
    rows = _rows(flight_states_enriched(clean_rel, airports_rel))
    baw = [r for r in rows if r["icao24"] == "400f53"]
    assert len(baw) == 1
    assert baw[0]["nearest_airport_icao"] == "EGLL"


def test_flight_phase_cruise(clean_rel, airports_rel):
    """DLH123 at ~36,000 ft → cruise."""
    rows = _rows(flight_states_enriched(clean_rel, airports_rel))
    dlh = [r for r in rows if r["icao24"] == "3c6444"]
    assert len(dlh) == 1
    assert dlh[0]["flight_phase"] == "cruise"


def test_flight_phase_descent(clean_rel, airports_rel):
    """AFR789 is descending (vertical_fpm < -500) → descent or approach."""
    rows = _rows(flight_states_enriched(clean_rel, airports_rel))
    afr = [r for r in rows if r["icao24"] == "3944ef"]
    if afr:
        assert afr[0]["flight_phase"] in ("descent", "approach")


def test_flight_phase_climb(clean_rel, airports_rel):
    """KLM012 is climbing (vertical_fpm > 500) → climb or departure."""
    rows = _rows(flight_states_enriched(clean_rel, airports_rel))
    klm = [r for r in rows if r["icao24"] == "484161"]
    if klm:
        assert klm[0]["flight_phase"] in ("climb", "departure")


def test_nearest_airport_km_positive(clean_rel, airports_rel):
    rows = _rows(flight_states_enriched(clean_rel, airports_rel))
    matched = [r for r in rows if r.get("nearest_airport_icao")]
    assert all(r["nearest_airport_km"] > 0 for r in matched)


def test_enriched_columns(clean_rel, airports_rel):
    result = flight_states_enriched(clean_rel, airports_rel)
    cols = set(result.columns)
    expected = {
        "icao24", "flight_phase", "nearest_airport_icao",
        "nearest_airport_name", "nearest_airport_country",
        "nearest_airport_km",
    }
    assert expected.issubset(cols)

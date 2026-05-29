"""OpenSky source — polls /states/all for the Europe bounding box every 30s."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.request import urlopen
from urllib.error import URLError

import duckdb

from conduit_etl import source, Table

log = logging.getLogger(__name__)

_OPENSKY_URL = (
    "https://opensky-network.org/api/states/all"
    "?lamin=35.0&lomin=-15.0&lamax=72.0&lomax=42.0"
)


def fetch_states(url: str = _OPENSKY_URL) -> dict[str, Any]:
    """HTTP GET the OpenSky API. Raises ConnectionError on failure."""
    try:
        with urlopen(url, timeout=15) as resp:
            return json.loads(resp.read())
    except URLError as exc:
        raise ConnectionError(f"OpenSky API unreachable: {exc}") from exc


def states_to_relation(
    data: dict[str, Any], server_time: int | None = None
) -> duckdb.DuckDBPyRelation:
    """Convert a raw /states/all payload dict to a DuckDB relation.

    Accepts either a real API response or a pre-loaded dict (for tests).
    """
    _COLS = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source",
    ]
    ts = server_time if server_time is not None else data.get("time", 0)
    states = data.get("states") or []

    _EMPTY = duckdb.sql(
        "SELECT '' AS icao24, '' AS callsign, '' AS origin_country, "
        "0 AS time_position, 0 AS last_contact, 0.0 AS longitude, "
        "0.0 AS latitude, 0.0 AS baro_altitude, false AS on_ground, "
        "0.0 AS velocity, 0.0 AS true_track, 0.0 AS vertical_rate, "
        "'' AS squawk, 0.0 AS geo_altitude, 0 AS position_source, "
        "0 AS server_time LIMIT 0"
    )

    if not states:
        return _EMPTY

    rows = []
    for s in states:
        row = {col: s[i] if i < len(s) else None for i, col in enumerate(_COLS)}
        row["server_time"] = ts
        if isinstance(row["callsign"], str):
            row["callsign"] = row["callsign"].strip()
        rows.append(row)

    con = duckdb.connect()
    con.execute("""
        CREATE TEMP TABLE _raw_states (
            icao24 VARCHAR, callsign VARCHAR, origin_country VARCHAR,
            time_position BIGINT, last_contact BIGINT,
            longitude DOUBLE, latitude DOUBLE, baro_altitude DOUBLE,
            on_ground BOOLEAN, velocity DOUBLE, true_track DOUBLE,
            vertical_rate DOUBLE, geo_altitude DOUBLE,
            squawk VARCHAR, position_source INTEGER, server_time BIGINT
        )
    """)
    con.executemany(
        "INSERT INTO _raw_states VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            [
                r["icao24"], r["callsign"], r["origin_country"],
                r.get("time_position"), r.get("last_contact"),
                r.get("longitude"), r.get("latitude"), r.get("baro_altitude"),
                r.get("on_ground", False), r.get("velocity"), r.get("true_track"),
                r.get("vertical_rate"), r.get("geo_altitude"),
                r.get("squawk"), r.get("position_source", 0), r["server_time"],
            ]
            for r in rows
        ],
    )
    return con.sql("SELECT * FROM _raw_states")


@source(schedule="30s", output="flight_states_raw")
def flight_states_raw() -> Table:
    """Fetch current aircraft positions over Europe from OpenSky."""
    try:
        data = fetch_states()
    except ConnectionError as exc:
        log.warning("skipping tick — OpenSky unreachable: %s", exc)
        return duckdb.sql(
            "SELECT '' AS icao24, '' AS callsign, '' AS origin_country, "
            "0 AS time_position, 0 AS last_contact, 0.0 AS longitude, "
            "0.0 AS latitude, 0.0 AS baro_altitude, false AS on_ground, "
            "0.0 AS velocity, 0.0 AS true_track, 0.0 AS vertical_rate, "
            "'' AS squawk, 0.0 AS geo_altitude, 0 AS position_source, "
            "0 AS server_time LIMIT 0"
        )
    return states_to_relation(data)

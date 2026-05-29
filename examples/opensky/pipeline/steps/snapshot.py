"""Airspace snapshot — one row per live aircraft, always current."""

from __future__ import annotations

from conduit_etl import step, Table


@step(output="airspace_snapshot", merge="upsert", merge_key=["icao24"])
def airspace_snapshot(flight_states_enriched: Table) -> Table:
    """Latest position for every tracked aircraft (upsert on icao24)."""
    return flight_states_enriched.query("enriched", """
        SELECT DISTINCT ON (icao24) *
        FROM enriched
        ORDER BY icao24, last_contact DESC
    """)

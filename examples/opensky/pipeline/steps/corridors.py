"""Busy corridors — 0.5°×0.5° grid density heatmap."""

from __future__ import annotations

from conduit_etl import step, Table


@step(
    output="busy_corridors",
    incremental=True,
    merge="upsert",
    merge_key=["grid_lat", "grid_lon", "window_start"],
)
def busy_corridors(flight_states_enriched: Table) -> Table:
    """Aircraft density per 0.5° grid square and 5-minute window."""
    return flight_states_enriched.query("enriched", """
        SELECT
            floor(latitude  / 0.5) * 0.5        AS grid_lat,
            floor(longitude / 0.5) * 0.5        AS grid_lon,
            time_bucket(INTERVAL '5 minutes', snapshot_at) AS window_start,
            count(DISTINCT icao24)               AS aircraft_count,
            ROUND(avg(altitude_ft))              AS avg_altitude_ft,
            ROUND(avg(speed_kts), 1)             AS avg_speed_kts,
            mode() WITHIN GROUP (ORDER BY flight_phase) AS dominant_phase
        FROM enriched
        GROUP BY grid_lat, grid_lon, window_start
    """)

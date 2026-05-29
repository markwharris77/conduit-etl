"""Airport traffic — per-airport aircraft counts in 5-minute windows."""

from __future__ import annotations

from conduit_etl import step, Table


@step(
    output="airport_traffic",
    incremental=True,
    merge="upsert",
    merge_key=["nearest_airport_icao", "window_start", "flight_phase"],
)
def airport_traffic(flight_states_enriched: Table) -> Table:
    """Count aircraft near each airport by phase and 5-minute window."""
    return flight_states_enriched.query("enriched", """
        SELECT
            nearest_airport_icao,
            nearest_airport_name,
            nearest_airport_country,
            time_bucket(INTERVAL '5 minutes', snapshot_at) AS window_start,
            flight_phase,
            count(DISTINCT icao24)               AS aircraft_count,
            ROUND(min(altitude_ft))              AS min_altitude_ft
        FROM enriched
        WHERE nearest_airport_icao IS NOT NULL
          AND nearest_airport_km < 100
        GROUP BY
            nearest_airport_icao,
            nearest_airport_name,
            nearest_airport_country,
            window_start,
            flight_phase
    """)

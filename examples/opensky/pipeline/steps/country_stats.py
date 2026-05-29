"""Country stats — rolling 5-minute aircraft counts by country and flight phase."""

from __future__ import annotations

from conduit_etl import step, Table


@step(
    output="country_stats",
    incremental=True,
    merge="upsert",
    merge_key=["origin_country", "window_start", "flight_phase"],
)
def country_stats(flight_states_enriched: Table) -> Table:
    """Count distinct aircraft per country, phase, and 5-minute window."""
    return flight_states_enriched.query("enriched", """
        SELECT
            origin_country,
            time_bucket(INTERVAL '5 minutes', snapshot_at) AS window_start,
            flight_phase,
            count(DISTINCT icao24)               AS aircraft_count,
            ROUND(avg(altitude_ft))              AS avg_altitude_ft,
            ROUND(avg(speed_kts), 1)             AS avg_speed_kts
        FROM enriched
        GROUP BY origin_country, window_start, flight_phase
    """)

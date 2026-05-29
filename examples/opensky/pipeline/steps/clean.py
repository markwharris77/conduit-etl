"""Clean and enrich raw flight states with derived columns."""

from __future__ import annotations

from conduit_etl import step, Table


@step(output="flight_states_clean", incremental=True, merge="append")
def flight_states_clean(flight_states_raw: Table) -> Table:
    """Filter to airborne aircraft and add human-friendly derived columns."""
    return flight_states_raw.query("raw", """
        SELECT
            icao24,
            callsign,
            origin_country,
            time_position,
            last_contact,
            longitude,
            latitude,
            baro_altitude,
            velocity,
            true_track,
            vertical_rate,
            geo_altitude,
            squawk,
            position_source,
            server_time,

            ROUND(baro_altitude * 3.28084)            AS altitude_ft,
            ROUND(velocity      * 1.94384, 1)         AS speed_kts,
            ROUND(vertical_rate * 196.85)              AS vertical_fpm,
            CAST(true_track AS INTEGER)               AS heading_deg,
            CASE
                WHEN true_track >= 315 OR true_track < 45  THEN 'N'
                WHEN true_track >= 45  AND true_track < 135 THEN 'E'
                WHEN true_track >= 135 AND true_track < 225 THEN 'S'
                ELSE 'W'
            END                                       AS heading_cardinal,
            -- epoch_ms (milliseconds) gives naive TIMESTAMP without timezone
            epoch_ms(CAST(last_contact AS BIGINT) * 1000) AS last_contact_at,
            epoch_ms(CAST(server_time  AS BIGINT) * 1000) AS snapshot_at,
            CASE position_source
                WHEN 0 THEN 'ADS-B'
                WHEN 1 THEN 'ASTERIX'
                WHEN 2 THEN 'MLAT'
                WHEN 3 THEN 'FLARM'
                ELSE 'UNKNOWN'
            END                                       AS position_source_name

        FROM raw
        WHERE on_ground     = false
          AND longitude     IS NOT NULL
          AND latitude      IS NOT NULL
          AND baro_altitude IS NOT NULL
          AND baro_altitude > 0
          AND velocity      IS NOT NULL
          AND velocity      > 0
          AND icao24 NOT IN ('000000', '')
    """)

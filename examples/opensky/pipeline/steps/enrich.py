"""Enrich clean flight states with nearest airport and flight phase."""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

from conduit_etl import step, Table


@step(output="flight_states_enriched", incremental=True, merge="append")
def flight_states_enriched(flight_states_clean: Table, airports_ref: Table) -> Table:
    """Join each aircraft to its nearest large/medium airport (within 370 km).

    Bridges the two inputs via temp parquet files (they may come from different
    DuckDB connections), then materialises the result into an in-memory table so
    the caller receives a self-contained relation with no dangling file references.
    """
    airports_path = result_path = None
    try:
        # Stage airports to parquet so the clean relation's connection can read it
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            airports_path = f.name
        airports_ref.write_parquet(airports_path)

        lazy = flight_states_clean.query("clean", f"""
            WITH airports AS (SELECT * FROM read_parquet('{airports_path}')),
            candidates AS (
                SELECT
                    f.*,
                    a.icao_code        AS nearest_airport_icao,
                    a.airport_name     AS nearest_airport_name,
                    a.country_code     AS nearest_airport_country,
                    a.region_code      AS nearest_airport_region,
                    ROUND(
                        111.0 * sqrt(
                            power((f.latitude  - a.latitude ), 2) +
                            power((f.longitude - a.longitude) * cos(radians(f.latitude)), 2)
                        ), 1
                    ) AS nearest_airport_km,
                    row_number() OVER (
                        PARTITION BY f.icao24, f.last_contact
                        ORDER BY
                            power((f.latitude  - a.latitude ), 2) +
                            power((f.longitude - a.longitude) * cos(radians(f.latitude)), 2)
                    ) AS _rn
                FROM clean f
                JOIN airports a
                  ON  a.latitude  BETWEEN f.latitude  - 3 AND f.latitude  + 3
                  AND a.longitude BETWEEN f.longitude - 4 AND f.longitude + 4
            ),
            nearest AS (
                SELECT * FROM candidates WHERE _rn = 1 AND nearest_airport_km <= 370
            ),
            unmatched AS (
                SELECT
                    f.*,
                    NULL AS nearest_airport_icao,
                    NULL AS nearest_airport_name,
                    NULL AS nearest_airport_country,
                    NULL AS nearest_airport_region,
                    NULL::DOUBLE AS nearest_airport_km
                FROM clean f
                WHERE NOT EXISTS (
                    SELECT 1 FROM candidates c
                    WHERE c.icao24 = f.icao24 AND c.last_contact = f.last_contact
                )
            ),
            combined AS (
                SELECT * EXCLUDE (_rn) FROM nearest
                UNION ALL
                SELECT * FROM unmatched
            )
            SELECT *,
                CASE
                    WHEN altitude_ft > 30000                                THEN 'cruise'
                    WHEN vertical_fpm > 500                                 THEN 'climb'
                    WHEN vertical_fpm < -500                                THEN 'descent'
                    WHEN altitude_ft < 5000
                         AND nearest_airport_km < 50
                         AND vertical_fpm < -200                            THEN 'approach'
                    WHEN altitude_ft < 5000
                         AND nearest_airport_km < 50
                         AND vertical_fpm > 200                             THEN 'departure'
                    ELSE 'level'
                END AS flight_phase
            FROM combined
        """)

        # Materialise to a temp parquet so the result has no dependency on airports_path
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            result_path = f.name
        lazy.write_parquet(result_path)

    finally:
        if airports_path:
            Path(airports_path).unlink(missing_ok=True)

    # Load result into a fresh in-memory connection — fully self-contained
    try:
        out_con = duckdb.connect()
        out_con.execute(
            f"CREATE TABLE enriched_data AS SELECT * FROM read_parquet('{result_path}')"
        )
        return out_con.table("enriched_data")
    finally:
        if result_path:
            Path(result_path).unlink(missing_ok=True)

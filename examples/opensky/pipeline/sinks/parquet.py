"""Parquet sink — write all output tables to ./output/ for offline analysis."""

from __future__ import annotations

import duckdb

from conduit_etl import sink, Table
from conduit_etl.sinks.parquet import write_parquet


@sink
def parquet_sink(
    airspace_snapshot: Table,
    country_stats: Table,
    airport_traffic: Table,
    busy_corridors: Table,
) -> Table:
    """Write analytical tables to ./output/<table>/ as Parquet files."""
    n1 = write_parquet(airspace_snapshot, "output/airspace_snapshot/latest.parquet")
    n2 = write_parquet(country_stats,     "output/country_stats/latest.parquet")
    n3 = write_parquet(airport_traffic,   "output/airport_traffic/latest.parquet")
    n4 = write_parquet(busy_corridors,    "output/busy_corridors/latest.parquet")
    return duckdb.sql(f"""
        SELECT 'airspace_snapshot' AS table_name, {n1} AS rows_written
        UNION ALL SELECT 'country_stats',   {n2}
        UNION ALL SELECT 'airport_traffic', {n3}
        UNION ALL SELECT 'busy_corridors',  {n4}
    """)

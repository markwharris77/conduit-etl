"""Airports reference source — downloads OurAirports CSV once daily."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

import duckdb

from conduit_etl import source, Table

log = logging.getLogger(__name__)

_AIRPORTS_URL = "https://ourairports.com/data/airports.csv"


def fetch_airports_csv(url: str = _AIRPORTS_URL) -> str:
    """Download the OurAirports CSV and return as a string."""
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except URLError as exc:
        raise ConnectionError(f"OurAirports unreachable: {exc}") from exc


def csv_to_relation(csv_text: str) -> duckdb.DuckDBPyRelation:
    """Parse OurAirports CSV text into a DuckDB relation.

    Writes to a temp file for reading, then materialises into an in-memory
    DuckDB table so the temp file can be safely deleted before returning.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(csv_text)
        tmp_path = tmp.name
    try:
        con = duckdb.connect()
        con.execute(f"""
            CREATE TABLE airports_data AS
            SELECT
                ident         AS icao_code,
                name          AS airport_name,
                iso_country   AS country_code,
                iso_region    AS region_code,
                municipality,
                type          AS airport_type,
                latitude_deg  AS latitude,
                longitude_deg AS longitude,
                elevation_ft,
                iata_code
            FROM read_csv('{tmp_path}', header=true)
            WHERE type IN ('large_airport', 'medium_airport')
              AND latitude_deg  IS NOT NULL
              AND longitude_deg IS NOT NULL
        """)
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    return con.table("airports_data")


@source(schedule="24h", output="airports_ref")
def airports_ref() -> Table:
    """Download and filter the OurAirports reference dataset."""
    try:
        csv_text = fetch_airports_csv()
    except ConnectionError as exc:
        log.warning("skipping airports refresh: %s", exc)
        return duckdb.sql(
            "SELECT '' AS icao_code, '' AS airport_name, '' AS country_code, "
            "'' AS region_code, '' AS municipality, '' AS airport_type, "
            "0.0 AS latitude, 0.0 AS longitude, 0 AS elevation_ft, "
            "'' AS iata_code LIMIT 0"
        )
    return csv_to_relation(csv_text)

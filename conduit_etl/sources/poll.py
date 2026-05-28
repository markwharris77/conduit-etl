"""PollSource — poll a SQL data source on a schedule with a high-water mark.

The decorated function is called with the last high-water mark value (or None
on the first run). It should return a DuckDB relation of new/changed rows since
that mark. The runtime writes those rows to the catalog and advances the mark.

Example:

    @source(schedule="hourly", output="raw_orders")
    def raw_orders(watermark: str | None) -> Table:
        since = watermark or "1970-01-01"
        con = duckdb.connect()
        con.execute("ATTACH 'postgres://...' AS pg (TYPE POSTGRES)")
        return con.sql(f"SELECT * FROM pg.orders WHERE updated_at > '{since}'")

The watermark value is stored in the step's last run record fingerprint under
``"__watermark__"``. The source function is responsible for interpreting it.
"""

from __future__ import annotations

from typing import Any

from conduit_etl.catalog.base import CatalogBackend


def get_watermark(step_name: str, catalog: CatalogBackend) -> Any:
    """Return the stored watermark for ``step_name``, or ``None`` on first run."""
    last = catalog.last_run(step_name, only_success=True)
    if last is None:
        return None
    return last.fingerprint.get("__watermark__")


def make_watermark_fingerprint(value: Any, base: dict) -> dict:
    """Embed a new watermark into a fingerprint dict."""
    return {**base, "__watermark__": value}

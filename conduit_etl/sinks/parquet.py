"""ParquetSink — write a table out to a parquet file on disk.

Phase 1 sink. The decorated function receives the input table as a DuckDB
relation and must write it to ``dest_path``. This is intentionally thin —
just a helper that the user's sink step calls to write its output.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from conduit_etl.core.errors import SinkError


def write_parquet(
    relation: duckdb.DuckDBPyRelation,
    dest_path: str | Path,
    *,
    overwrite: bool = True,
) -> int:
    """Write ``relation`` to ``dest_path`` as parquet. Returns row count."""
    dest = Path(dest_path).expanduser()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        raise SinkError(f"parquet file already exists and overwrite=False: {dest}")
    try:
        relation.write_parquet(str(dest))
    except Exception as exc:
        raise SinkError(f"failed to write parquet to {dest}: {exc}") from exc
    return int(relation.aggregate("count(*) AS n").fetchone()[0])

"""PostgresSink — upsert or append rows to a PostgreSQL table.

Requires the ``postgres`` extra: ``pip install conduit-etl[postgres]``.

DuckDB's built-in postgres_scanner extension is used for the write path.

Example:

    from conduit_etl import sink, Table
    from conduit_etl.sinks.postgres import write_postgres

    @sink
    def orders_pg(clean_orders: Table) -> None:
        write_postgres(
            clean_orders,
            conn_str="postgresql://user:pw@host/db",
            table="orders",
            merge="upsert",
            merge_key=["order_id"],
        )
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Literal


def write_postgres(
    relation: any,
    *,
    conn_str: str,
    table: str,
    schema: str = "public",
    merge: Literal["replace", "append", "upsert"] = "append",
    merge_key: list[str] | None = None,
) -> int:
    """Write ``relation`` to a PostgreSQL table. Returns row count written.

    Uses DuckDB's postgres_scanner extension. The relation is first materialised
    to a temp parquet file so it can be read by a fresh DuckDB connection without
    hitting cross-connection restrictions (no pyarrow required).
    """
    import duckdb

    # Materialise to temp parquet so a fresh connection can read it.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        relation.write_parquet(tmp_path)

        con = duckdb.connect()
        try:
            con.execute("INSTALL postgres_scanner")
            con.execute("LOAD postgres_scanner")
            con.execute(f"ATTACH '{conn_str}' AS pg (TYPE POSTGRES)")

            full_table = f'pg."{schema}"."{table}"'
            stage = f"read_parquet('{tmp_path}')"

            if merge == "replace":
                con.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM {stage}")
            elif merge == "append":
                con.execute(f"INSERT INTO {full_table} SELECT * FROM {stage}")
            elif merge == "upsert":
                if not merge_key:
                    raise ValueError("upsert merge requires merge_key")
                on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in merge_key)
                con.execute(
                    f'DELETE FROM {full_table} AS t '
                    f'USING {stage} AS s WHERE {on_clause}'
                )
                con.execute(f"INSERT INTO {full_table} SELECT * FROM {stage}")
            else:
                raise ValueError(f"unknown merge mode: {merge!r}")

            rows = con.execute(f"SELECT count(*) FROM {stage}").fetchone()[0]
            return int(rows)
        finally:
            con.close()
    finally:
        Path(tmp_path).unlink(missing_ok=True)

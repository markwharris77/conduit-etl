"""PostgresSink — upsert or append rows to a PostgreSQL table.

Requires the ``postgres`` extra: ``pip install conduit-etl[postgres]``.

DuckDB's built-in postgres scanner is used to write data, avoiding a separate
psycopg dependency for the write path. psycopg is only used for DDL operations
(creating tables if they don't exist).

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

from typing import Literal

try:
    import duckdb as _duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False


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

    Uses DuckDB's postgres_scanner extension for the write path.
    """
    import duckdb

    # Use a fresh connection for the write so it doesn't interfere with the catalog.
    con = duckdb.connect()
    try:
        con.execute("INSTALL postgres_scanner")
        con.execute("LOAD postgres_scanner")
        con.execute(f"ATTACH '{conn_str}' AS pg (TYPE POSTGRES)")
        con.register("_conduit_pg_stage", relation.arrow() if hasattr(relation, "arrow") else relation)

        full_table = f'pg.{schema}."{table}"'

        if merge == "replace":
            con.execute(f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM _conduit_pg_stage")
        elif merge == "append":
            con.execute(f"INSERT INTO {full_table} SELECT * FROM _conduit_pg_stage")
        elif merge == "upsert":
            if not merge_key:
                raise ValueError("upsert merge requires merge_key")
            keys = merge_key
            on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
            con.execute(
                f'DELETE FROM {full_table} AS t USING _conduit_pg_stage AS s WHERE {on_clause}'
            )
            con.execute(f"INSERT INTO {full_table} SELECT * FROM _conduit_pg_stage")
        else:
            raise ValueError(f"unknown merge mode: {merge!r}")

        rows = con.execute("SELECT count(*) FROM _conduit_pg_stage").fetchone()[0]
        return int(rows)
    finally:
        con.close()

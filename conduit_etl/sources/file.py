"""FileSource — glob a directory, hash-dedup, return new/changed files as a relation.

Each call returns only files that are new or changed since the last run.
The watermark is a JSON dict of ``{path: sha256}`` stored in the run fingerprint.

Example:

    from conduit_etl import source, Table
    from conduit_etl.sources.file import file_batch

    @source(schedule="always", output="raw_csv")
    def raw_csv() -> Table:
        return file_batch("/data/incoming/*.csv", format="csv")
"""

from __future__ import annotations

import glob
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


def _file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def file_batch(
    pattern: str,
    *,
    format: str = "parquet",
    previous_hashes: dict[str, str] | None = None,
    columns: dict[str, str] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation of all new/changed files matching ``pattern``.

    ``previous_hashes`` maps path → sha256 from the last run. Files whose
    hash hasn't changed are excluded. Pass ``None`` on the first run to read
    all matching files.

    Returns a relation plus sets ``file_batch.new_hashes`` for the caller to
    persist as the next watermark.
    """
    paths = sorted(glob.glob(pattern, recursive=True))
    prev = previous_hashes or {}

    new_paths = []
    new_hashes: dict[str, str] = {}
    for p in paths:
        h = _file_hash(p)
        new_hashes[p] = h
        if prev.get(p) != h:
            new_paths.append(p)

    # Attach new_hashes to the function so the caller can read it
    file_batch.new_hashes = new_hashes  # type: ignore[attr-defined]

    if not new_paths:
        # Return an empty relation with a _file_path column
        return duckdb.sql("SELECT '' AS _file_path LIMIT 0")

    fmt = format.lower()
    if fmt == "parquet":
        quoted = ", ".join(f"'{p}'" for p in new_paths)
        rel = duckdb.sql(
            f"SELECT *, '' AS _file_path FROM read_parquet([{quoted}])"
        )
        # Add actual path column
        pieces = [duckdb.sql(f"SELECT *, '{p}' AS _file_path FROM read_parquet('{p}')") for p in new_paths]
        return _union_all(pieces)
    elif fmt in ("csv", "tsv"):
        sep = "\t" if fmt == "tsv" else ","
        pieces = [
            duckdb.sql(f"SELECT *, '{p}' AS _file_path FROM read_csv('{p}', sep='{sep}', header=true)")
            for p in new_paths
        ]
        return _union_all(pieces)
    elif fmt == "json":
        pieces = [
            duckdb.sql(f"SELECT *, '{p}' AS _file_path FROM read_json('{p}')")
            for p in new_paths
        ]
        return _union_all(pieces)
    else:
        raise ValueError(f"unsupported file format: {format!r}")


def _union_all(relations: list[duckdb.DuckDBPyRelation]) -> duckdb.DuckDBPyRelation:
    result = relations[0]
    for rel in relations[1:]:
        result = result.union(rel)
    return result


def get_file_hashes(step_name: str, catalog: Any) -> dict[str, str]:
    """Retrieve the stored file hash map for a step (its last watermark)."""
    last = catalog.last_run(step_name, only_success=True)
    if last is None:
        return {}
    raw = last.fingerprint.get("__file_hashes__")
    if raw is None:
        return {}
    return raw if isinstance(raw, dict) else json.loads(raw)

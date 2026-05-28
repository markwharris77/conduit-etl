"""Input fingerprinting and skip logic.

A fingerprint captures what a step's last successful execution saw: each input
table's snapshot id and row count, plus a hash of the step function's source.
If the next tick computes the same fingerprint, the step is skipped.

The ``__fn_hash__`` entry guarantees that a code change re-runs the step even
when the input data is unchanged.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from conduit_etl.catalog.base import CatalogBackend
    from conduit_etl.core.models import Step


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def schema_hash(relation: duckdb.DuckDBPyRelation) -> str:
    """Stable 16-hex-char hash of a relation's column names and types."""
    cols = list(zip(relation.columns, [str(t) for t in relation.types], strict=True))
    return _sha256(json.dumps(cols, sort_keys=True))[:16]


def fn_hash(source: str) -> str:
    """Hash of a step function's source code — drives re-run on code change."""
    return _sha256(source)[:16]


def compute_fingerprint(step: Step, catalog: CatalogBackend) -> dict[str, Any]:
    """Build the fingerprint for ``step`` against the catalog's current state.

    Returns ``{input_name: [snapshot_id, rows], ..., "__fn_hash__": str}``. If
    any required input table is missing a snapshot, that input's entry is
    ``None`` and downstream code should treat the step as not-ready rather than
    re-running.
    """
    fp: dict[str, Any] = {"__fn_hash__": fn_hash(step.fn_source)}
    for name in step.input_names:
        snap = catalog.latest_snapshot(name)
        fp[name] = [snap.id, snap.rows] if snap is not None else None
    return fp


def fingerprint_changed(
    previous: dict[str, Any] | None, current: dict[str, Any]
) -> bool:
    """True if the step needs to re-run (previous missing or differs)."""
    if previous is None:
        return True
    # Strip ``__meta__`` if present in the stored fingerprint — it isn't part
    # of the identity comparison.
    prev = {k: v for k, v in previous.items() if k != "__meta__"}
    curr = {k: v for k, v in current.items() if k != "__meta__"}
    return prev != curr

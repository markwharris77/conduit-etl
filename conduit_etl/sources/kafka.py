"""KafkaSource — Confluent Kafka consumer group micro-batch source.

Requires the ``kafka`` extra: ``pip install conduit-etl[kafka]``.

Example:

    from conduit_etl import source, Table
    from conduit_etl.sources.kafka import kafka_batch

    @source(schedule="always", output="raw_events")
    def raw_events() -> Table:
        return kafka_batch("my-topic", brokers=["kafka:9092"], max_messages=500)
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

try:
    from confluent_kafka import Consumer, KafkaException  # type: ignore[import]
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False


def _require_kafka() -> None:
    if not _KAFKA_AVAILABLE:
        raise ImportError(
            "confluent-kafka is required for KafkaSource. "
            "Install it with: pip install conduit-etl[kafka]"
        )


def kafka_batch(
    topic: str,
    *,
    brokers: list[str],
    group_id: str = "conduit-etl",
    max_messages: int = 1000,
    poll_timeout: float = 1.0,
    value_deserializer: Any = None,
) -> Any:
    """Consume up to ``max_messages`` from ``topic`` and return as a DuckDB relation.

    Each message value is expected to be a JSON object. The relation will have
    one column per JSON key, plus ``_kafka_offset`` and ``_kafka_partition``.
    Returns an empty relation if no messages are available.

    Consumer group offsets are NOT committed here — commit them after the
    catalog write succeeds to get at-least-once delivery semantics.
    """
    _require_kafka()
    import duckdb

    consumer = Consumer({
        "bootstrap.servers": ",".join(brokers),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    records: list[dict] = []
    try:
        while len(records) < max_messages:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            value = msg.value()
            if value_deserializer:
                row = value_deserializer(value)
            else:
                row = json.loads(value.decode("utf-8") if isinstance(value, bytes) else value)
            if isinstance(row, dict):
                row["_kafka_offset"] = msg.offset()
                row["_kafka_partition"] = msg.partition()
            records.append(row)
    finally:
        consumer.close()

    if not records:
        return duckdb.sql("SELECT 1 LIMIT 0")

    # Write to a temp NDJSON file and read back — portable across DuckDB versions.
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".ndjson", delete=False, encoding="utf-8"
    ) as f:
        tmp = f.name
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")

    try:
        rel = duckdb.read_json(tmp)
    finally:
        Path(tmp).unlink(missing_ok=True)

    return rel

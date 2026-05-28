"""KafkaSink — publish rows from a DuckDB relation to a Kafka topic.

Requires the ``kafka`` extra: ``pip install conduit-etl[kafka]``.

Each row is serialised as a JSON object. A custom serialiser can be
provided for Avro, Protobuf, or other formats.

Example:

    from conduit_etl import sink, Table
    from conduit_etl.sinks.kafka import write_kafka

    @sink
    def publish_orders(clean_orders: Table) -> None:
        write_kafka(clean_orders, topic="orders", brokers=["kafka:9092"])
"""

from __future__ import annotations

import json
from typing import Callable

try:
    from confluent_kafka import Producer, KafkaException  # type: ignore[import]
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False


def _require_kafka() -> None:
    if not _KAFKA_AVAILABLE:
        raise ImportError(
            "confluent-kafka is required for KafkaSink. "
            "Install it with: pip install conduit-etl[kafka]"
        )


def write_kafka(
    relation: any,
    *,
    topic: str,
    brokers: list[str],
    key_column: str | None = None,
    serializer: Callable[[dict], bytes] | None = None,
    batch_size: int = 500,
    flush_timeout: float = 30.0,
) -> int:
    """Publish all rows in ``relation`` to a Kafka topic. Returns row count published.

    ``key_column`` names a column whose value becomes the Kafka message key.
    ``serializer`` converts a row dict to bytes (defaults to UTF-8 JSON).
    """
    _require_kafka()

    producer = Producer({"bootstrap.servers": ",".join(brokers)})
    columns = relation.columns
    rows = relation.fetchall()
    count = 0
    errors: list[Exception] = []

    def _delivery(err, msg):
        if err:
            errors.append(KafkaException(err))

    def _default_serialize(row_dict: dict) -> bytes:
        return json.dumps(row_dict, default=str).encode("utf-8")

    serialize = serializer or _default_serialize

    for row in rows:
        row_dict = dict(zip(columns, row))
        key = str(row_dict[key_column]).encode() if key_column and key_column in row_dict else None
        value = serialize(row_dict)
        producer.produce(topic, value=value, key=key, on_delivery=_delivery)
        count += 1
        if count % batch_size == 0:
            producer.poll(0)

    producer.flush(flush_timeout)

    if errors:
        raise errors[0]

    return count

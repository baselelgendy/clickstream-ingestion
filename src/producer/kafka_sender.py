"""Thin Kafka producer wrapper: send(table, record).

The Kafka client is created lazily on the first send, so importing this module
never needs a running broker (and tests can run without kafka-python installed).
"""
import json
import logging

from config import settings
from src.common.schema import STREAM_TABLES

log = logging.getLogger(__name__)

_producer = None
_errors = 0


def _on_error(exc):
    global _errors
    _errors += 1
    log.error("Kafka send failed: %s", exc)


def _get_producer():
    global _producer
    if _producer is None:
        from kafka import KafkaProducer  # imported here on purpose (lazy)

        _producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            acks="all",                      # wait for the broker to confirm
            retries=5,
            linger_ms=50,                    # small batching window = higher throughput
            batch_size=64 * 1024,
            compression_type="gzip",
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
    return _producer


def send(table: str, record: dict) -> None:
    """Publish one record of `table` (events, cart_items, ...) to its topic.

    The message key is the table's key column (session_id for events), so all
    events of one session stay in the same partition and keep their order.
    """
    key_col = STREAM_TABLES[table][1]
    _get_producer().send(
        settings.topic_for(table), key=record.get(key_col), value=record
    ).add_errback(_on_error)


def flush() -> None:
    if _producer is not None:
        _producer.flush()


def close() -> int:
    """Flush and close. Returns the number of messages that failed to send."""
    global _producer
    if _producer is not None:
        _producer.flush()
        _producer.close()
        _producer = None
    return _errors

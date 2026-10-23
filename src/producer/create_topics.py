"""Create the Kafka topics (one per streamed table). Safe to run many times.

Topic names come from the same table list the producer/consumer use (schema.py), so they
can never drift apart. Waits for the broker to come up, which is what makes
`docker compose up` work without any manual ordering.

  python -m src.producer.create_topics
"""
import logging
import time

from config import settings
from src.common.schema import STREAM_TABLES

log = logging.getLogger("create_topics")


def wanted_topics() -> list:
    return [settings.topic_for(t) for t in STREAM_TABLES]


def missing_topics(existing) -> list:
    existing = set(existing)
    return [t for t in wanted_topics() if t not in existing]


def main(wait_seconds: int = 90):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from kafka.admin import KafkaAdminClient, NewTopic
    # Only KafkaError is imported: specific names (NoBrokersAvailable, ...) differ between
    # kafka-python versions, KafkaError exists in all of them.
    from kafka.errors import KafkaError

    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
            break
        except KafkaError as exc:          # broker not up yet (NoBrokersAvailable & co.)
            if time.monotonic() > deadline:
                raise SystemExit(f"Kafka not reachable at {settings.KAFKA_BOOTSTRAP_SERVERS} "
                                 f"after {wait_seconds}s: {exc!r}")
            log.info("Waiting for Kafka at %s (%s) ...", settings.KAFKA_BOOTSTRAP_SERVERS,
                     type(exc).__name__)
            time.sleep(3)

    try:
        todo = missing_topics(admin.list_topics())
        for name in todo:
            try:
                admin.create_topics([NewTopic(name, settings.KAFKA_PARTITIONS, 1)])
                log.info("created topic %s (%d partitions)", name, settings.KAFKA_PARTITIONS)
            except KafkaError as exc:
                if "TopicAlreadyExists" not in type(exc).__name__:
                    raise
        log.info("Topics ready: %s", wanted_topics())
    finally:
        admin.close()


if __name__ == "__main__":
    main()
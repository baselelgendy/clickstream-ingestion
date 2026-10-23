"""Kafka -> S3 writer (raw layer).

Reads the 5 clickstream topics, buffers messages per table and writes one file per
table per flush:

  s3://<bucket>/raw/clickstream/<table>/year=YYYY/month=MM/day=DD/<table>-<ts>-<id>.json

One JSON object per line (JSON Lines), so Spark / Athena / Glue read it directly.
Partitions use the *ingestion* date; the event's own time stays in `event_ts`.

Delivery is at-least-once: offsets are committed only after every S3 write of the
flush succeeded. If the process dies in between, some messages are written twice, so
de-duplicate on event_id (events) / order_id (orders) in the Spark job.

Run from the project root:   python -m src.consumer.s3_writer
Stop with Ctrl+C: it flushes what is buffered, commits, then exits.
"""
import gzip
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from config import settings
from src.common.schema import STREAM_TABLES

log = logging.getLogger("s3_writer")


def build_key(prefix: str, table: str, now: datetime, compress: bool, uid: str = None) -> str:
    uid = uid or uuid.uuid4().hex[:8]
    ext = "json.gz" if compress else "json"
    return (f"{prefix}/{table}/year={now:%Y}/month={now:%m}/day={now:%d}/"
            f"{table}-{now:%Y%m%dT%H%M%S}-{uid}.{ext}")


def serialize(rows: list, compress: bool) -> bytes:
    body = ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode("utf-8")
    return gzip.compress(body) if compress else body


class S3Writer:
    def __init__(self, s3_client, bucket: str, prefix: str, compress: bool = False,
                 attempts: int = 3, sleep=time.sleep):
        self.s3, self.bucket, self.prefix, self.compress = s3_client, bucket, prefix, compress
        self.attempts, self.sleep = attempts, sleep

    def write(self, table: str, rows: list) -> str:
        """Upload one file. A transient network error (timeout, reset) is retried with a
        growing pause instead of killing the consumer; only after `attempts` failures it raises
        (the offsets are then not committed, Kafka re-delivers after the restart)."""
        body = serialize(rows, self.compress)
        for attempt in range(1, self.attempts + 1):
            key = build_key(self.prefix, table, datetime.now(timezone.utc), self.compress)
            try:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
                break
            except Exception as exc:                      # botocore errors, timeouts, ...
                if attempt == self.attempts:
                    raise
                wait = 5 * attempt
                log.warning("S3 upload of %s failed (%s: %s), attempt %d/%d, retrying in %ds",
                            table, type(exc).__name__, str(exc)[:120], attempt, self.attempts, wait)
                self.sleep(wait)
        log.info("wrote %6d rows -> s3://%s/%s (%.0f KB)", len(rows), self.bucket, key, len(body) / 1024)
        return key


def run(consumer, writer, batch_size, flush_seconds, stop=lambda: False,
        clock=time.monotonic, idle_exit=0):
    """Poll -> buffer per table -> flush to S3 -> commit offsets.

    idle_exit: 0 = run until stopped. N > 0 = return after N seconds without any new
    message, but only once at least one message has been received (so the consumer can
    start before the producer without quitting early).
    """
    buffers = defaultdict(list)
    last_flush = last_msg = clock()
    seen_any = False

    def flush_all():
        nonlocal last_flush
        for table in list(buffers):
            if buffers[table]:
                writer.write(table, buffers[table])
                buffers[table] = []
        consumer.commit()               # only after all S3 writes succeeded
        last_flush = clock()

    try:
        while not stop():
            polled = consumer.poll(timeout_ms=1000)
            for tp, msgs in polled.items():
                buffers[settings.table_for(tp.topic)].extend(m.value for m in msgs)
            if polled:
                seen_any, last_msg = True, clock()

            has_data = any(buffers.values())
            full = any(len(b) >= batch_size for b in buffers.values())
            timed_out = clock() - last_flush >= flush_seconds
            if has_data and (full or timed_out):
                flush_all()
            elif timed_out:
                last_flush = clock()

            if idle_exit and seen_any and clock() - last_msg >= idle_exit:
                log.info("No new messages for %ds, finishing.", idle_exit)
                break
    except KeyboardInterrupt:
        log.info("Stopping, flushing buffered messages...")
    # Normal stop (or Ctrl+C): flush what is left. On any other exception we do NOT
    # flush or commit, so Kafka re-delivers the uncommitted messages on restart.
    if any(buffers.values()):
        flush_all()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import boto3
    from botocore.config import Config
    from kafka import KafkaConsumer

    bucket = settings.require_bucket()
    topics = [settings.topic_for(t) for t in STREAM_TABLES]
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=2000,
        max_poll_interval_ms=600_000,   # a slow S3 upload must not get the consumer kicked out
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    s3 = boto3.client("s3", region_name=settings.AWS_REGION,
                      config=Config(connect_timeout=10, read_timeout=30,
                                    retries={"max_attempts": 3, "mode": "standard"}))
    writer = S3Writer(s3, bucket, settings.S3_FACT_PREFIX, settings.S3_COMPRESS)
    log.info("Consuming %s -> s3://%s/%s (batch=%d rows, flush=%ds)", topics, bucket,
             settings.S3_FACT_PREFIX, settings.BATCH_SIZE, settings.FLUSH_SECONDS)
    try:
        run(consumer, writer, settings.BATCH_SIZE, settings.FLUSH_SECONDS,
            idle_exit=settings.IDLE_EXIT_SECONDS)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
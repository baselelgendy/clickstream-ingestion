"""Unit tests for the replay, consumer and uploader logic.

They need neither Kafka, nor AWS, nor kafka-python/boto3 (fakes are used).
Run from the project root:  python -m unittest discover -s tests -v
"""
import csv
import gzip
import json
import tempfile
import unittest
from collections import Counter, namedtuple
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from config import settings
from src.common.schema import cast_row
from src.consumer import s3_writer
from src.producer import create_topics
from src.batch import prepare_data
from src.batch import upload_dimensions
from src.producer.replay_events import replay


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def make_dataset(folder: Path, break_child=False):
    """6 events: types 6,1,3,7,8,4 -> one child row each for 1,3,7,8."""
    write_csv(folder / "08_events.csv",
              ["event_id", "session_id", "event_type_id", "product_id", "coupon_code",
               "error_code", "event_ts", "price", "latency"],
              [["E1", "S1", 6, "P1", "", "", "2020-01-01 10:00:00", 5.5, 100],
               ["E2", "S1", 1, "P1", "", "", "2020-01-01 10:00:05", 5.5, 120],
               ["E3", "S2", 3, "P2", "SAVE10", "", "2020-01-01 10:01:00", 7.0, 90],
               ["E4", "S2", 7, "P2", "", "PAYMENT_ERR_2", "2020-01-01 10:02:00", 7.0, ""],
               ["E5", "S3", 8, "P3", "", "", "2020-01-01 10:03:00", 9.0, ""],
               ["E6", "S3", 4, "P3", "", "", "2020-01-01 10:04:00", 9.0, ""]])
    write_csv(folder / "09_cart_items.csv", ["event_id", "quantity", "cart_value"], [["E2", 1, 5.5]])
    write_csv(folder / "10_checkouts.csv", ["event_id", "cart_value", "shipping_fee"],
              [["E9" if break_child else "E3", 7.0, 25]])
    write_csv(folder / "11_payment_attempts.csv", ["event_id", "amount", "status"], [["E4", 7.0, "failed"]])
    write_csv(folder / "12_orders.csv",
              ["order_id", "event_id", "session_id", "customer_id", "product_id", "coupon_code", "order_value"],
              [["O1", "E5", "S3", "", "P3", "", 9.0]])


class CastTests(unittest.TestCase):
    def test_nulls_and_types(self):
        row = cast_row("events", {"event_id": "E1", "coupon_code": "", "error_code": "nan",
                                  "event_type_id": "6", "price": "5.5", "latency": "100.0"})
        self.assertIsNone(row["coupon_code"])
        self.assertIsNone(row["error_code"])
        self.assertEqual(row["event_type_id"], 6)
        self.assertEqual(row["price"], 5.5)
        self.assertEqual(row["latency"], 100)

    def test_missing_latency_is_null(self):
        self.assertIsNone(cast_row("events", {"latency": ""})["latency"])


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.sent = []

    def tearDown(self):
        self._tmp.cleanup()

    def send(self, table, record):
        self.sent.append((table, record))

    def test_child_rows_follow_their_event(self):
        make_dataset(self.dir)
        counts = replay(self.dir, self.send)
        self.assertEqual(counts, Counter(events=6, cart_items=1, checkouts=1,
                                         payment_attempts=1, orders=1))
        tables = [t for t, _ in self.sent]
        self.assertEqual(tables, ["events", "events", "cart_items", "events", "checkouts",
                                  "events", "payment_attempts", "events", "orders", "events"])
        # a child row always comes right after the event with the same event_id
        for i, (t, rec) in enumerate(self.sent):
            if t != "events":
                self.assertEqual(self.sent[i - 1][1]["event_id"], rec["event_id"])

    def test_out_of_sync_child_file_stops_replay(self):
        make_dataset(self.dir, break_child=True)
        with self.assertRaisesRegex(RuntimeError, "out of sync"):
            replay(self.dir, self.send)

    def test_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            replay(self.dir, self.send)

    def test_limit(self):
        make_dataset(self.dir)
        counts = replay(self.dir, self.send, limit=3)
        self.assertEqual(counts["events"], 3)
        self.assertEqual((counts["cart_items"], counts["checkouts"], counts["orders"]), (1, 1, 0))
        self.assertEqual([r["event_id"] for t, r in self.sent if t == "events"], ["E1", "E2", "E3"])

    def test_skip_resumes_and_keeps_children_aligned(self):
        make_dataset(self.dir)
        replay(self.dir, self.send, skip=3)   # E1..E3 skipped (E2, E3 had children)
        ids = [r["event_id"] for t, r in self.sent if t == "events"]
        self.assertEqual(ids, ["E4", "E5", "E6"])
        self.assertEqual([t for t, _ in self.sent if t != "events"], ["payment_attempts", "orders"])

    def test_counts_survive_interruption(self):
        make_dataset(self.dir)
        counts = Counter()

        def boom(table, record):
            if record["event_id"] == "E3":
                raise KeyboardInterrupt
            self.send(table, record)

        with self.assertRaises(KeyboardInterrupt):
            replay(self.dir, boom, counts=counts)
        self.assertEqual(counts["events"], 2)   # E1, E2 fully sent -> resume with --skip 2

    def test_rate_limits_speed(self):
        make_dataset(self.dir)
        now = [0.0]
        slept = []

        def sleep(s):
            slept.append(s)
            now[0] += s

        replay(self.dir, self.send, rate=2, sleep=sleep, clock=lambda: now[0])
        # 6 events at 2/s must take ~3 seconds of (fake) time
        self.assertAlmostEqual(sum(slept), 2.5, delta=0.6)


class FakeConsumer:
    def __init__(self, batches):
        self.batches, self.commits, self.polls = list(batches), 0, 0

    def poll(self, timeout_ms=0):
        self.polls += 1
        return self.batches.pop(0) if self.batches else {}

    def commit(self):
        self.commits += 1


class FakeS3:
    def __init__(self, fail=False):
        self.objects, self.fail = {}, fail

    def put_object(self, Bucket, Key, Body):
        if self.fail:
            raise RuntimeError("AccessDenied")
        self.objects[(Bucket, Key)] = Body


TopicPartition = namedtuple("TopicPartition", "topic partition")   # same shape as kafka-python's


def msgs(topic, rows):
    tp = TopicPartition(topic, 0)
    return {tp: [SimpleNamespace(value=r) for r in rows]}


class ConsumerTests(unittest.TestCase):
    def test_build_key(self):
        now = datetime(2026, 10, 23, 1, 2, 3, tzinfo=timezone.utc)
        key = s3_writer.build_key("raw/clickstream", "events", now, False, uid="abcd1234")
        self.assertEqual(key, "raw/clickstream/events/year=2026/month=10/day=23/"
                              "events-20261023T010203-abcd1234.json")
        self.assertTrue(s3_writer.build_key("p", "orders", now, True).endswith(".json.gz"))

    def test_flush_on_batch_size_one_file_per_table_then_commit(self):
        s3 = FakeS3()
        writer = s3_writer.S3Writer(s3, "bkt", "raw/clickstream")
        ev = settings.topic_for("events")
        od = settings.topic_for("orders")
        consumer = FakeConsumer([msgs(ev, [{"event_id": "E1"}, {"event_id": "E2"}]),
                                 msgs(od, [{"order_id": "O1"}])])
        polls = iter([False, False, True])
        s3_writer.run(consumer, writer, batch_size=2, flush_seconds=999, stop=lambda: next(polls))
        keys = sorted(k for _, k in s3.objects)
        self.assertEqual(len(keys), 2)                       # events flushed (full), orders on shutdown
        self.assertTrue(keys[0].startswith("raw/clickstream/events/"))
        self.assertTrue(keys[1].startswith("raw/clickstream/orders/"))
        body = next(v for (_, k), v in s3.objects.items() if "/events/" in k).decode()
        self.assertEqual([json.loads(l)["event_id"] for l in body.splitlines()], ["E1", "E2"])
        self.assertTrue(body.endswith("\n"))
        self.assertEqual(consumer.commits, 2)

    def test_flush_on_timeout(self):
        s3 = FakeS3()
        writer = s3_writer.S3Writer(s3, "bkt", "raw/clickstream")
        consumer = FakeConsumer([msgs(settings.topic_for("events"), [{"event_id": "E1"}])])
        t = iter([0, 0, 100, 100, 100])                      # clock jumps past FLUSH_SECONDS
        stops = iter([False, True])
        s3_writer.run(consumer, writer, batch_size=10_000, flush_seconds=60,
                      stop=lambda: next(stops), clock=lambda: next(t))
        self.assertEqual(len(s3.objects), 1)
        self.assertEqual(consumer.commits, 1)

    def test_gzip_roundtrip(self):
        rows = [{"a": 1}, {"a": "é"}]
        data = s3_writer.serialize(rows, compress=True)
        self.assertEqual([json.loads(l) for l in gzip.decompress(data).decode().splitlines()], rows)

    def test_s3_failure_does_not_commit(self):
        writer = s3_writer.S3Writer(FakeS3(fail=True), "bkt", "raw/clickstream")
        consumer = FakeConsumer([msgs(settings.topic_for("events"), [{"event_id": "E1"}])])
        with self.assertRaises(RuntimeError):
            s3_writer.run(consumer, writer, batch_size=1, flush_seconds=60, stop=lambda: False)
        self.assertEqual(consumer.commits, 0)                # Kafka will re-deliver after restart


class IdleExitTests(unittest.TestCase):
    def _run(self, idle_exit, batches, ticks):
        s3 = FakeS3()
        consumer = FakeConsumer(batches)
        t = iter(ticks)
        last = [0]

        def clock():
            last[0] = next(t, last[0])
            return last[0]

        s3_writer.run(consumer, s3_writer.S3Writer(s3, "b", "raw/clickstream"), 10_000, 9999,
                      stop=lambda: consumer.polls > 50, clock=clock, idle_exit=idle_exit)
        return s3, consumer

    def test_exits_after_idle_and_flushes(self):
        ev = settings.topic_for("events")
        s3, c = self._run(30, [msgs(ev, [{"event_id": "E1"}])], [0, 0, 1, 1, 1, 1, 40, 40, 40, 40, 40])
        self.assertEqual(len(s3.objects), 1)       # buffered row flushed on exit
        self.assertEqual(c.commits, 1)
        self.assertLess(c.polls, 50)               # it stopped by itself

    def test_does_not_exit_before_first_message(self):
        # no messages at all: must keep polling (stops only because the test's stop() fires)
        s3, c = self._run(30, [], [0] + [100] * 200)
        self.assertGreater(c.polls, 50)
        self.assertEqual(len(s3.objects), 0)


class AutomationTests(unittest.TestCase):
    def test_topics_to_create_match_tables(self):
        self.assertEqual(len(create_topics.wanted_topics()), 5)
        self.assertEqual(create_topics.missing_topics([]), create_topics.wanted_topics())
        have = [settings.topic_for("events"), "other"]
        self.assertNotIn(settings.topic_for("events"), create_topics.missing_topics(have))
        self.assertEqual(len(create_topics.missing_topics(have)), 4)

    def test_csvs_ready_logic(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            self.assertFalse(prepare_data.csvs_ready(d, "small"))
            for f in prepare_data.required_files():
                (d / f).write_text("x\n1\n")
            self.assertEqual(len(prepare_data.required_files()), 12)
            self.assertTrue(prepare_data.csvs_ready(d, "small"))          # no marker = trust existing
            (d / prepare_data.MARKER).write_text("small")
            self.assertTrue(prepare_data.csvs_ready(d, "small"))
            self.assertFalse(prepare_data.csvs_ready(d, "full"))          # size changed -> regenerate
            (d / "08_events.csv").write_text("")
            self.assertFalse(prepare_data.csvs_ready(d, "small"))         # empty file = not ready

    def test_kaggle_ready_and_command(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            self.assertFalse(prepare_data.kaggle_ready(d))
            (d / "a.csv").write_text("x")
            self.assertTrue(prepare_data.kaggle_ready(d))
        small = prepare_data.generator_command("small", Path("in"), Path("out"))
        full = prepare_data.generator_command("full", Path("in"), Path("out"))
        self.assertIn("--events", small)
        self.assertNotIn("--events", full)


class UploadPlanTests(unittest.TestCase):
    def test_plan_reports_missing_files(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "07_products.csv").write_text("product_id\n1\n")
            items, missing = upload_dimensions.plan(Path(d), "raw/dimensions")
            self.assertEqual(items[0][2], "raw/dimensions/products/07_products.csv")
            self.assertEqual(len(missing), 6)
            items, missing = upload_dimensions.plan(Path(d), "raw/dimensions", only=["products"])
            self.assertEqual((len(items), len(missing)), (1, 0))


if __name__ == "__main__":
    unittest.main()

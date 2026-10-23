"""Replay the generated CSVs into Kafka as a live clickstream.

The teammate's generator writes 12 CSV tables in one batch. This script turns the
event tables into a stream:

  08_events.csv ──► topic clickstream.events
  09_cart_items.csv        ┐
  10_checkouts.csv         │ each child row is sent right after the event it belongs to
  11_payment_attempts.csv  │ (same event_id) to its own topic
  12_orders.csv            ┘

The generator writes child rows in the same order as their events, so the files are
read in lock-step (no big lookup dict in RAM). Every child row is checked against its
event_id and the replay stops loudly if they ever drift apart.

Run from the project root:
  python -m src.producer.replay_events                 # use REPLAY_RATE from .env
  python -m src.producer.replay_events --rate 1000
  python -m src.producer.replay_events --limit 5000 --dry-run
  python -m src.producer.replay_events --skip 120000   # resume after a stop
"""
import argparse
import csv
import json
import logging
import time
from collections import Counter
from pathlib import Path

from config import settings
from src.common.schema import CHILD_TABLES, STREAM_TABLES, cast_row

log = logging.getLogger("replay")

# event_type_id -> child table that holds the extra columns of that event
EVENT_TYPE_TO_CHILD = {1: "cart_items", 3: "checkouts", 7: "payment_attempts", 8: "orders"}


def _open_readers(input_dir: Path):
    files, readers = [], {}
    try:
        for table in ["events", *CHILD_TABLES]:
            path = input_dir / STREAM_TABLES[table][0]
            if not path.exists():
                raise FileNotFoundError(
                    f"{path} not found. Run the generator first (scripts/generate_data.ps1)."
                )
            f = open(path, newline="", encoding="utf-8")
            files.append(f)
            readers[table] = csv.DictReader(f)
    except Exception:
        for f in files:
            f.close()
        raise
    return files, readers


def replay(input_dir, send, rate=0.0, limit=None, skip=0, log_every=10.0,
           sleep=time.sleep, clock=time.monotonic, counts=None):
    """Send events (+ child rows) through `send(table, record)`.

    rate  : events per second, 0 = as fast as possible
    limit : stop after this many events (None = all)
    skip  : ignore the first N events (resume), child files are advanced accordingly
    counts: optional Counter that is updated in place, so the caller still has
            the totals if the replay is interrupted (Ctrl+C).
    Returns the Counter of messages sent per table.
    """
    files, readers = _open_readers(Path(input_dir))
    counts = Counter() if counts is None else counts
    sent = 0
    pace_every = max(1, int(rate // 20)) if rate > 0 else 0   # check the clock ~20x/sec
    t0 = last_log = None
    try:
        for idx, ev in enumerate(readers["events"]):
            child_table = EVENT_TYPE_TO_CHILD.get(int(ev["event_type_id"]))
            child = None
            if child_table:
                child = next(readers[child_table], None)
                if child is None or child["event_id"] != ev["event_id"]:
                    got = None if child is None else child["event_id"]
                    raise RuntimeError(
                        f"{child_table} is out of sync with events at event "
                        f"{ev['event_id']} (child row has {got}). Re-generate the CSVs."
                    )
            if idx < skip:
                continue
            if limit is not None and sent >= limit:
                break

            send("events", cast_row("events", ev))
            counts["events"] += 1
            if child:
                send(child_table, cast_row(child_table, child))
                counts[child_table] += 1
            sent += 1

            now = clock()
            if t0 is None:
                t0 = last_log = now
            if rate > 0 and sent % pace_every == 0:
                ahead = sent / rate - (now - t0)
                if ahead > 0:
                    sleep(ahead)
            if now - last_log >= log_every:
                last_log = now
                log.info("sent %s events (%.0f/s) | %s", f"{sent:,}",
                         sent / max(now - t0, 1e-9), dict(counts))
    finally:
        for f in files:
            f.close()
    return counts


class _JsonlSender:
    """--dry-run: write messages to logs/dry_run/<table>.jsonl instead of Kafka."""

    def __init__(self, folder: Path):
        folder.mkdir(parents=True, exist_ok=True)
        self.files = {t: open(folder / f"{t}.jsonl", "w", encoding="utf-8") for t in STREAM_TABLES}

    def __call__(self, table, record):
        self.files[table].write(json.dumps(record, ensure_ascii=False) + "\n")

    def close(self):
        for f in self.files.values():
            f.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", default=str(settings.GENERATED_CSV_DIR))
    ap.add_argument("--rate", type=float, default=settings.REPLAY_RATE,
                    help="events per second (0 = max speed)")
    ap.add_argument("--limit", type=int, default=None, help="stop after N events")
    ap.add_argument("--skip", type=int, default=0, help="skip the first N events (resume)")
    ap.add_argument("--dry-run", action="store_true",
                    help="write to logs/dry_run/*.jsonl instead of Kafka")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.dry_run:
        sender = _JsonlSender(settings.PROJECT_ROOT / "logs" / "dry_run")
        send, close = sender, sender.close
    else:
        from src.producer import kafka_sender
        send, close = kafka_sender.send, kafka_sender.close

    counts = Counter()
    try:
        replay(args.input_dir, send, rate=args.rate, limit=args.limit,
               skip=args.skip, counts=counts)
    except KeyboardInterrupt:
        log.warning("Stopped by user.")
    finally:
        failed = close()
    done = counts["events"]
    log.info("Done. Messages sent: %s", dict(counts))
    log.info("To resume later: --skip %d", args.skip + done)
    if failed:
        raise SystemExit(f"{failed} messages failed to send, check the log above.")


if __name__ == "__main__":
    main()

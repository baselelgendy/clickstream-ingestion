# Clickstream Ingestion: generator -> Kafka -> S3

```
Kaggle CSVs ──► generator (12 CSV tables) ──┬─► replay_events.py ──► Kafka (5 topics) ──► s3_writer.py ──► S3 raw/clickstream/<table>/...
                                            └─► upload_dimensions.py (7 static tables) ─────────────────► S3 raw/dimensions/<table>/...
```

| Part | Tables | How it reaches S3 |
|---|---|---|
| Streamed | `events`, `cart_items`, `checkouts`, `payment_attempts`, `orders` | replay -> Kafka topic `clickstream.<table>` -> consumer -> JSON Lines in S3 |
| Static dimensions | `customers`, `devices`, `calendar_days`, `sessions`, `event_types`, `categories`, `products` | uploaded once as CSV |

## Quick start (automated, recommended)

Needs only **Docker Desktop** and your AWS keys (`aws configure`, once). No Python, no venv.

1. Put the Kaggle CSV files in `data\kaggle_data\`
   (https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop).
2. Check `.env`: `S3_BUCKET`, and `GENERATE_SIZE=small` (quick check) or `full` (10M events).
3. Run **one command**:

```powershell
.\scripts\start.ps1            # or:  docker compose up -d --build
```

That single command does, in order: generate + validate the 12 CSVs (only if they do not exist yet)
-> start Kafka -> create the 5 topics -> start the consumer -> replay the events -> upload the 7
dimension tables. Stop with `.\scripts\stop.ps1` (`-Reset` also wipes Kafka's stored messages).

Useful while it runs:

```powershell
docker compose ps                         # who is running / finished
docker compose logs -f consumer           # S3 writes: "wrote 5000 rows -> s3://..."
docker compose logs replay                # how many messages were sent
```

Run-once mode: set `IDLE_EXIT_SECONDS=30` in `.env` and the consumer ends by itself 30 s after the last
message, so the whole `docker compose up` finishes cleanly (handy for a scheduler that waits for completion).
With `0` (default) the consumer keeps running like a real streaming job.

Which container does what: `prepare` (generate CSVs), `kafka` + `kafka-init` (broker + topics),
`consumer` (Kafka -> S3), `replay` (CSV -> Kafka), `dimensions` (static tables -> S3). All use the same image
(`Dockerfile`) and the same `.env`. Containers reach Kafka at `kafka:19092`; your own computer uses `localhost:9092`.

## Folder layout

```
.env / .env.example      all settings (bucket, region, topics, batch size, replay rate); AWS keys are NOT here
config/settings.py       reads .env, nothing is hard-coded in the code
Dockerfile               one image for all pipeline steps (Python 3.11)
docker-compose.yml       the whole pipeline: kafka, kafka-init, prepare, consumer, replay, dimensions
generator/               teammate's generator + validator (one small patch, see below)
data/kaggle_data/        put the Kaggle CSVs here        (git-ignored)
data/generated_csvs/     generator output                (git-ignored)
src/common/schema.py     table definitions + CSV -> JSON typing
src/producer/            kafka_sender.py (send), replay_events.py (CSV -> Kafka), create_topics.py
src/consumer/s3_writer.py   Kafka -> S3
src/batch/               upload_dimensions.py (dimension CSVs -> S3), prepare_data.py (generate CSVs if missing)
scripts/                 start.ps1 / stop.ps1 (automated), create_topics.ps1 / generate_data.ps1 (manual mode)
tests/                   unit tests (no Kafka/AWS needed)
logs/                    dry-run output
```

## Manual mode (without Docker, for debugging)

### One-time setup (PowerShell, from the project root)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
aws sts get-caller-identity          # must show ingestion-user
python -m unittest discover -s tests -t .   # optional: 20 tests should pass
```

Kaggle data: download https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop
and unzip the CSVs into `data\kaggle_data\`.

### Run order

```powershell
# 1. generate + validate the CSVs (start with -Small to check everything works)
.\scripts\generate_data.ps1 -Small
.\scripts\generate_data.ps1                    # full size: 10M events

# 2. start Kafka and create the topics
docker compose up -d kafka
.\scripts\create_topics.ps1

# 3. dimensions to S3 (once)
python -m src.batch.upload_dimensions

# 4. terminal A: consumer (leave it running)
python -m src.consumer.s3_writer

# 5. terminal B: replay the events as a live stream
python -m src.producer.replay_events                  # speed = REPLAY_RATE in .env
python -m src.producer.replay_events --rate 2000
python -m src.producer.replay_events --limit 5000     # only the first 5000 events
python -m src.producer.replay_events --dry-run --limit 100   # no Kafka, writes logs/dry_run/*.jsonl

# check S3
aws s3 ls s3://<bucket>/raw/ --recursive --human-readable | more
```

Stop with Ctrl+C. The producer prints `To resume later: --skip N`; the consumer flushes and commits
before exiting. When you finish a session: `docker compose down`.

## S3 layout

```
raw/clickstream/events/year=2026/month=10/day=23/events-20261023T101500-1a2b3c4d.json
raw/clickstream/orders/year=.../month=.../day=.../orders-....json
raw/dimensions/products/07_products.csv
```

JSON Lines, one object per row, `null` for empty values. Partitions use the **ingestion** date;
the event time is the `event_ts` column.

## Things to know

- **Duplicates:** delivery is at-least-once. If the consumer is killed between an S3 write and its commit,
  some rows are written twice. De-duplicate on `event_id` (events) and `order_id` (orders) in Spark.
- **Out of order:** the generator assigns `event_ts` randomly inside each session's window, so rows are
  not sorted by time. Spark should rely on `event_ts`, not on arrival order.
- **Speed:** 10M events + ~1.3M child rows. At `REPLAY_RATE=200` the replay takes about 14 hours;
  at 2000 about 1.5 hours. Use `--limit` for demos.
- **File size:** `BATCH_SIZE` / `FLUSH_SECONDS` decide the file size (one file per table per flush). Larger
  files are better for Spark/Athena; very small values create thousands of tiny files.
- **Consumer group:** the consumer remembers its position (`KAFKA_GROUP_ID`). To re-read everything, change
  the group id in `.env`.
- **Generator patch:** `generator/generate_clickstream_kaggle.py` crashed on the real Kaggle `event_time`
  format (`... UTC`, timezone-aware vs naive comparison). One line was patched (marked `PATCH`).
  Tell the teammate so their copy gets the same fix.

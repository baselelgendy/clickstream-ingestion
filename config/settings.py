"""Central configuration. Every value comes from .env (see .env.example)."""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}


def _path(value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else PROJECT_ROOT / p


# --- AWS / S3 ---
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_FACT_PREFIX = os.getenv("S3_FACT_PREFIX", "raw/clickstream").strip("/")
S3_DIM_PREFIX = os.getenv("S3_DIM_PREFIX", "raw/dimensions").strip("/")
S3_COMPRESS = _bool("S3_COMPRESS")

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "clickstream")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "s3-writer")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", 3))

# --- Consumer batching ---
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", 60))
# 0 = keep running forever (real streaming). N > 0 = after the first message, exit once no
# message arrived for N seconds (flushes and commits first). Useful for "run once and finish".
IDLE_EXIT_SECONDS = int(os.getenv("IDLE_EXIT_SECONDS", 0))

# --- Producer ---
KAGGLE_CSV_DIR = _path(os.getenv("KAGGLE_CSV_DIR", "data/kaggle_data"))
GENERATED_CSV_DIR = _path(os.getenv("GENERATED_CSV_DIR", "data/generated_csvs"))
GENERATE_SIZE = os.getenv("GENERATE_SIZE", "small").strip().lower()   # small | full
REPLAY_RATE = float(os.getenv("REPLAY_RATE", 200))


def topic_for(table: str) -> str:
    """clickstream.events, clickstream.orders, ..."""
    return f"{KAFKA_TOPIC_PREFIX}.{table}"


def table_for(topic: str) -> str:
    return topic.removeprefix(f"{KAFKA_TOPIC_PREFIX}.")


def require_bucket() -> str:
    if not S3_BUCKET:
        raise SystemExit("S3_BUCKET is empty. Set it in .env")
    return S3_BUCKET

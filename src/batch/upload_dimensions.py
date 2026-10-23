"""Upload the 7 dimension CSVs (customers, devices, calendar_days, sessions,
event_types, categories, products) to S3 once, as-is:

  s3://<bucket>/raw/dimensions/<table>/<csv file>

These tables are static reference data, so they do not go through Kafka.

Run from the project root:
  python -m src.batch.upload_dimensions
  python -m src.batch.upload_dimensions --only products sessions
  python -m src.batch.upload_dimensions --dry-run
"""
import argparse
import logging
from pathlib import Path

from config import settings
from src.common.schema import DIMENSION_TABLES

log = logging.getLogger("upload_dimensions")


def plan(input_dir: Path, prefix: str, only=None):
    """Return [(table, local_path, s3_key)] and the list of missing files."""
    items, missing = [], []
    for table, filename in DIMENSION_TABLES.items():
        if only and table not in only:
            continue
        path = input_dir / filename
        if path.exists():
            items.append((table, path, f"{prefix}/{table}/{filename}"))
        else:
            missing.append(str(path))
    return items, missing


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-dir", default=str(settings.GENERATED_CSV_DIR))
    ap.add_argument("--only", nargs="*", choices=list(DIMENSION_TABLES), help="upload only these tables")
    ap.add_argument("--dry-run", action="store_true", help="show what would be uploaded")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    items, missing = plan(Path(args.input_dir), settings.S3_DIM_PREFIX, args.only)
    if missing:
        raise SystemExit("Missing files (run the generator first):\n  " + "\n  ".join(missing))

    if args.dry_run:
        for table, path, key in items:
            log.info("[dry-run] %s (%.1f MB) -> %s", path.name, path.stat().st_size / 1e6, key)
        return

    import boto3
    bucket = settings.require_bucket()
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    for table, path, key in items:
        log.info("uploading %s (%.1f MB) -> s3://%s/%s", path.name, path.stat().st_size / 1e6, bucket, key)
        s3.upload_file(str(path), bucket, key)       # multipart for big files (sessions)
    log.info("Done: %d files uploaded.", len(items))


if __name__ == "__main__":
    main()

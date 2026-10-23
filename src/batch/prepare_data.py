"""Make sure the 12 generated CSVs exist; run the teammate's generator + validator if not.

Used by `docker compose up` as the first step. Idempotent: if the CSVs are already there
(for the same GENERATE_SIZE) it does nothing, so restarting the pipeline is fast.

  python -m src.batch.prepare_data             # generate only if needed
  python -m src.batch.prepare_data --force     # always regenerate
"""
import argparse
import subprocess
import sys
from pathlib import Path

from config import settings
from src.common.schema import DIMENSION_TABLES, STREAM_TABLES

MARKER = ".generated_size"

SMALL_ARGS = ["--events", "10000", "--sessions", "1000", "--devices", "600", "--customers", "150",
              "--cart-items", "700", "--checkouts", "300", "--payment-attempts", "200",
              "--orders", "100", "--products", "100", "--categories", "10", "--calendar-days", "30"]


def required_files() -> list:
    return [f for f, _, _ in STREAM_TABLES.values()] + list(DIMENSION_TABLES.values())


def csvs_ready(out_dir: Path, size: str) -> bool:
    """All 12 files exist and were not generated for a different size."""
    if not all((out_dir / f).is_file() and (out_dir / f).stat().st_size > 0 for f in required_files()):
        return False
    marker = out_dir / MARKER
    return (not marker.exists()) or marker.read_text().strip() == size


def kaggle_ready(in_dir: Path) -> bool:
    return in_dir.is_dir() and any(p.suffix.lower() in (".csv", ".zip") for p in in_dir.rglob("*"))


def generator_command(size: str, in_dir: Path, out_dir: Path) -> list:
    cmd = [sys.executable, str(settings.PROJECT_ROOT / "generator" / "generate_clickstream_kaggle.py"),
           "--input", str(in_dir), "--output-dir", str(out_dir)]
    return cmd + (SMALL_ARGS if size == "small" else [])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    size, in_dir, out_dir = settings.GENERATE_SIZE, settings.KAGGLE_CSV_DIR, settings.GENERATED_CSV_DIR
    if size not in ("small", "full"):
        raise SystemExit(f"GENERATE_SIZE must be 'small' or 'full', got '{size}'")

    if not args.force and csvs_ready(out_dir, size):
        print(f"[prepare_data] CSVs already generated in {out_dir} (size={size}), skipping.")
        return
    if not kaggle_ready(in_dir):
        raise SystemExit(
            f"[prepare_data] No Kaggle CSV/ZIP found in {in_dir}.\n"
            "Download https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop "
            "and put the CSV files in data/kaggle_data/ (on your computer), then run again.")

    print(f"[prepare_data] generating '{size}' dataset from {in_dir} ...")
    subprocess.run(generator_command(size, in_dir, out_dir), check=True, cwd=settings.PROJECT_ROOT)
    subprocess.run([sys.executable, str(settings.PROJECT_ROOT / "generator" / "validate_clickstream_csvs.py"),
                    "--input-dir", str(out_dir)], check=True, cwd=settings.PROJECT_ROOT)
    (out_dir / MARKER).write_text(size)
    print("[prepare_data] done.")


if __name__ == "__main__":
    main()

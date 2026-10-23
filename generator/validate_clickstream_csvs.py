#!/usr/bin/env python3
"""Validate row counts and key relationships for generated Clickstream CSVs."""
import argparse
import csv
from pathlib import Path
import pandas as pd


def row_count(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)


def read_ids(path, column):
    return set(pd.read_csv(path, usecols=[column], dtype=str)[column].dropna())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default="generated_csvs")
    args = ap.parse_args()
    d = Path(args.input_dir)
    required = [
        "01_customers.csv", "02_devices.csv", "03_calendar_days.csv", "04_sessions.csv",
        "05_event_types.csv", "06_categories.csv", "07_products.csv", "08_events.csv",
        "09_cart_items.csv", "10_checkouts.csv", "11_payment_attempts.csv", "12_orders.csv"
    ]
    missing = [name for name in required if not (d / name).exists()]
    if missing:
        raise SystemExit(f"Missing files: {missing}")

    print("Row counts")
    for name in required:
        print(f"{name:28s} {row_count(d / name):>12,}")

    sessions = read_ids(d / "04_sessions.csv", "session_id")
    anonymous_ids = read_ids(d / "02_devices.csv", "anonymous_id")
    customers = read_ids(d / "01_customers.csv", "customer_id")
    products = read_ids(d / "07_products.csv", "product_id")
    categories = read_ids(d / "06_categories.csv", "category_id")

    errors = []
    devices = pd.read_csv(d / "02_devices.csv", dtype=str)
    bad = devices[devices["customer_id"].notna() & (devices["customer_id"] != "") & ~devices["customer_id"].isin(customers)]
    if len(bad): errors.append(f"devices.customer_id has {len(bad)} invalid references")

    sess_df = pd.read_csv(d / "04_sessions.csv", dtype=str)
    bad = sess_df[~sess_df["anonymous_id"].isin(anonymous_ids)]
    if len(bad): errors.append(f"sessions.anonymous_id has {len(bad)} invalid references")
    calendar = read_ids(d / "03_calendar_days.csv", "date_key")
    bad = sess_df[~sess_df["date_key"].isin(calendar)]
    if len(bad): errors.append(f"sessions.date_key has {len(bad)} invalid references")

    prod_df = pd.read_csv(d / "07_products.csv", dtype=str)
    bad = prod_df[~prod_df["category_id"].isin(categories)]
    if len(bad): errors.append(f"products.category_id has {len(bad)} invalid references")

    # Event IDs are sequential E000000001 ... E#########; validate child references without
    # loading all 10M event IDs into memory.
    event_n = row_count(d / "08_events.csv")
    def event_id_valid(value):
        if not isinstance(value, str) or not value.startswith("E"):
            return False
        try:
            n = int(value[1:])
            return 1 <= n <= event_n
        except ValueError:
            return False

    for filename in ["09_cart_items.csv", "10_checkouts.csv", "11_payment_attempts.csv", "12_orders.csv"]:
        child = pd.read_csv(d / filename, usecols=["event_id"], dtype=str)
        invalid = child[~child["event_id"].map(event_id_valid)]
        if len(invalid): errors.append(f"{filename}.event_id has {len(invalid)} invalid references")

    # Check event foreign keys in chunks so the validator remains memory efficient.
    for chunk in pd.read_csv(d / "08_events.csv", usecols=["session_id", "product_id"], dtype=str, chunksize=250_000):
        bad_s = chunk[~chunk["session_id"].isin(sessions)]
        if len(bad_s): errors.append(f"events.session_id has invalid references (at least {len(bad_s)})"); break
        bad_p = chunk[chunk["product_id"].notna() & ~chunk["product_id"].isin(products)]
        if len(bad_p): errors.append(f"events.product_id has invalid references (at least {len(bad_p)})"); break

    orders = pd.read_csv(d / "12_orders.csv", dtype=str)
    for col, valid_set in [("session_id", sessions), ("customer_id", customers), ("product_id", products)]:
        present = orders[col].notna() & (orders[col] != "")
        bad = orders[present & ~orders[col].isin(valid_set)]
        if len(bad): errors.append(f"orders.{col} has {len(bad)} invalid references")

    if errors:
        print("\nVALIDATION FAILED")
        for err in errors: print(f"- {err}")
        raise SystemExit(1)
    print("\nVALIDATION PASSED: required tables and checked relationships are valid.")

if __name__ == "__main__":
    main()

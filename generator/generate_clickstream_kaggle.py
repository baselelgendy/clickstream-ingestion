#!/usr/bin/env python3
"""Build the 12 Clickstream & Abandoned Cart CSV tables from the Kaggle cosmetics dataset.

Input may be a directory containing the monthly Kaggle CSVs, or a ZIP downloaded from Kaggle.
The default target creates 10,000,000 events and related tables in batches to limit RAM use.

Install:  pip install pandas numpy
Download: kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop --unzip -p kaggle_data
Run:      python generate_clickstream_kaggle.py --input kaggle_data --output-dir generated_csvs
Small test: python generate_clickstream_kaggle.py --input kaggle_data --output-dir test_csvs \
              --events 10000 --sessions 1000 --devices 600 --customers 150 \
              --cart-items 700 --checkouts 300 --payment-attempts 200 --orders 100 \
              --products 100

Note: Kaggle contains view/cart/remove_from_cart/purchase events, not explicit checkout or
payment-attempt events. Checkout and payment rows are simulated. The generator labels event
ID 2 as cart_remove to represent the source dataset faithfully; adjust EVENT_TYPES if your
organization requires coupon_applied instead.
"""
from __future__ import annotations
import argparse
import csv
import random
import zipfile
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

SEED = 42
# Eight event types; IDs align with the ERD's intended event type IDs, except ID 2 is named
# cart_remove because that is a real event in the Kaggle source dataset.
EVENT_TYPES = [
    (1, "add_to_cart", 2),
    (2, "cart_remove", 2),
    (3, "checkout_started", 4),
    (4, "session_end", 8),
    (5, "login", 1),
    (6, "product_view", 1),
    (7, "payment_attempt", 5),
    (8, "purchase", 6),
]

SOURCE_TYPE_TO_ID = {
    "cart": 1,
    "remove_from_cart": 2,
    "view": 6,
    "purchase": 8,
}

KAGGLE_COLUMNS = ["event_time", "event_type", "product_id", "category_id", "category_code",
                  "brand", "price", "user_id", "user_session"]


def source_csv_paths(input_path: Path):
    """Yield CSV file handles from a directory or ZIP archive."""
    if input_path.is_dir():
        paths = sorted(p for p in input_path.rglob("*.csv") if p.is_file())
        if not paths:
            raise FileNotFoundError(f"No CSV files found under {input_path}")
        for path in paths:
            yield path.name, path
    elif input_path.is_file() and input_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(input_path) as zf:
            names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv") and not n.endswith("/"))
            if not names:
                raise FileNotFoundError(f"No CSV files found in {input_path}")
            # ZipExtFile is valid only while the ZipFile is open, so read each member here.
            for name in names:
                with zf.open(name) as f:
                    yield name, f
    elif input_path.is_file() and input_path.suffix.lower() == ".csv":
        yield input_path.name, input_path
    else:
        raise ValueError("--input must be a Kaggle CSV, a directory of CSVs, or a .zip file")


def scan_source(input_path: Path, sample_per_type: int, chunksize: int, rng: np.random.Generator):
    """Stream source CSVs; collect catalog metadata and bounded samples per source event type."""
    reservoirs = {name: [] for name in SOURCE_TYPE_TO_ID}
    seen = defaultdict(int)
    products = {}  # product_id -> category_code/category_id/brand/price
    category_paths = set()
    total_rows = 0

    for filename, handle in source_csv_paths(input_path):
        # pandas handles both disk files and ZIP member streams.
        try:
            reader = pd.read_csv(handle, chunksize=chunksize, low_memory=False)
            for chunk in reader:
                chunk.columns = [str(c).strip() for c in chunk.columns]
                # Accept the user's earlier 11_events.csv as a fallback source format.
                if "event_time" not in chunk.columns and "event_ts" in chunk.columns:
                    chunk = chunk.rename(columns={
                        "event_ts": "event_time", "unit_price": "price", "session_id": "user_session",
                        "customer_id": "user_id", "product_id": "product_id"
                    })
                    if "event_type" not in chunk.columns:
                        type_map = {1: "cart", 2: "remove_from_cart", 3: "checkout_started",
                                    4: "session_end", 5: "login", 6: "view", 7: "payment_attempt", 8: "purchase"}
                        chunk["event_type"] = pd.to_numeric(chunk.get("event_type_id"), errors="coerce").map(type_map)
                    if "category_id" not in chunk: chunk["category_id"] = "unknown"
                    if "category_code" not in chunk: chunk["category_code"] = "unknown"
                    if "brand" not in chunk: chunk["brand"] = "unknown"
                missing = set(KAGGLE_COLUMNS) - set(chunk.columns)
                if missing:
                    # Ignore unrelated CSVs inside a directory/zip (e.g. metadata files).
                    if total_rows == 0:
                        continue
                    continue
                chunk = chunk[KAGGLE_COLUMNS]
                total_rows += len(chunk)
                for row in chunk.itertuples(index=False, name=None):
                    event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session = row
                    et = str(event_type).strip().lower() if pd.notna(event_type) else ""
                    if et in reservoirs:
                        seen[et] += 1
                        rec = {
                            "event_time": event_time,
                            "event_type": et,
                            "product_id": str(product_id) if pd.notna(product_id) else None,
                            "category_id_raw": str(category_id) if pd.notna(category_id) else None,
                            "category_code": str(category_code) if pd.notna(category_code) else None,
                            "brand": str(brand) if pd.notna(brand) else None,
                            "price": float(price) if pd.notna(price) else np.nan,
                            "user_id_raw": str(user_id) if pd.notna(user_id) else None,
                            "user_session_raw": str(user_session) if pd.notna(user_session) else None,
                        }
                        bucket = reservoirs[et]
                        if len(bucket) < sample_per_type:
                            bucket.append(rec)
                        else:
                            j = int(rng.integers(0, seen[et]))
                            if j < sample_per_type:
                                bucket[j] = rec
                    if pd.notna(product_id):
                        pid = str(product_id)
                        if pid not in products:
                            products[pid] = {
                                "category_id_raw": str(category_id) if pd.notna(category_id) else None,
                                "category_code": str(category_code) if pd.notna(category_code) else None,
                                "brand": str(brand) if pd.notna(brand) else None,
                                "price": float(price) if pd.notna(price) else np.nan,
                            }
                        else:
                            old = products[pid]
                            if (old["category_code"] is None or old["category_code"] == "nan") and pd.notna(category_code):
                                old["category_code"] = str(category_code)
                            if pd.notna(price) and (not np.isfinite(old["price"])):
                                old["price"] = float(price)
                    if pd.notna(category_code): category_paths.add(str(category_code))
        finally:
            # Files yielded from a ZIP are managed by source_csv_paths; disk files need no action.
            pass
    if not any(reservoirs.values()):
        raise ValueError("No supported event rows found. Expected Kaggle columns: " + ", ".join(KAGGLE_COLUMNS))
    return reservoirs, products, sorted(category_paths), dict(seen), total_rows


def sample_record(reservoirs, event_name, rng):
    bucket = reservoirs.get(event_name, [])
    if not bucket:
        # If the source lacks a particular event type, borrow a view/cart/purchase row.
        for fallback in ("view", "cart", "purchase", "remove_from_cart"):
            if reservoirs.get(fallback):
                bucket = reservoirs[fallback]
                break
    return bucket[int(rng.integers(0, len(bucket)))] if bucket else {}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Kaggle CSV directory, ZIP, or CSV file")
    p.add_argument("--output-dir", default="generated_csvs")
    p.add_argument("--events", type=int, default=10_000_000)
    p.add_argument("--sessions", type=int, default=1_000_000)
    p.add_argument("--devices", type=int, default=600_000)
    p.add_argument("--customers", type=int, default=150_000)
    p.add_argument("--cart-items", type=int, default=700_000)
    p.add_argument("--checkouts", type=int, default=300_000)
    p.add_argument("--payment-attempts", type=int, default=200_000)
    p.add_argument("--orders", type=int, default=100_000)
    p.add_argument("--products", type=int, default=10_000)
    p.add_argument("--categories", type=int, default=30)
    p.add_argument("--calendar-days", type=int, default=730)
    p.add_argument("--batch-size", type=int, default=100_000)
    p.add_argument("--sample-per-type", type=int, default=50_000)
    p.add_argument("--chunksize", type=int, default=100_000)
    p.add_argument("--seed", type=int, default=SEED)
    args = p.parse_args()

    if min(args.events, args.sessions, args.devices, args.customers, args.products,
           args.categories, args.calendar_days, args.batch_size) < 1:
        p.error("Counts and batch size must be positive")
    required_child_events = args.cart_items + args.checkouts + args.payment_attempts + args.orders
    if required_child_events > args.events:
        p.error("cart-items + checkouts + payment-attempts + orders cannot exceed --events")

    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    input_path = Path(args.input)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("Scanning source CSV(s) and building a bounded event sample/catalog...")
    reservoirs, product_meta, source_categories, source_counts, total_source_rows = scan_source(
        input_path, args.sample_per_type, args.chunksize, rng
    )
    print(f"Scanned {total_source_rows:,} supported source rows. Source event types: {source_counts}")

    # Categories: use source paths when available, then add placeholders to the requested count.
    category_names = source_categories[:args.categories]
    if len(category_names) < args.categories:
        category_names += [f"synthetic_category_{i:03d}" for i in range(len(category_names) + 1, args.categories + 1)]
    category_names = list(dict.fromkeys(category_names))[:args.categories]
    if not category_names:
        category_names = [f"category_{i:03d}" for i in range(1, args.categories + 1)]
    cat_id_by_name = {name: i + 1 for i, name in enumerate(category_names)}
    categories = pd.DataFrame({"category_id": list(cat_id_by_name.values()), "category_path": list(cat_id_by_name.keys())})
    categories.to_csv(out / "06_categories.csv", index=False)

    # Products: retain source product IDs and metadata as far as possible, then pad if necessary.
    all_product_ids = list(product_meta.keys())
    rng.shuffle(all_product_ids)
    if len(all_product_ids) >= args.products:
        chosen_product_ids = all_product_ids[:args.products]
    else:
        chosen_product_ids = all_product_ids + [f"SYNTH_P{i:07d}" for i in range(1, args.products - len(all_product_ids) + 1)]
    product_rows = []
    product_prices = {}
    for pid in chosen_product_ids:
        meta = product_meta.get(pid, {})
        cname = meta.get("category_code")
        if cname not in cat_id_by_name:
            cname = category_names[int(rng.integers(0, len(category_names)))]
        price = meta.get("price", np.nan)
        if not np.isfinite(price) or price < 0:
            price = float(rng.integers(100, 5000)) / 100
        product_prices[str(pid)] = float(price)
        product_rows.append({"product_id": str(pid), "category_id": cat_id_by_name[cname],
                             "product_name": (meta.get("brand") or "") + (" " if meta.get("brand") else "") + f"Product {pid}",
                             "list_price": round(float(price), 2)})
    products = pd.DataFrame(product_rows)
    products.to_csv(out / "07_products.csv", index=False)
    valid_products = products["product_id"].astype(str).to_numpy()
    valid_product_set = set(valid_products.tolist())
    price_by_product = dict(zip(products["product_id"].astype(str), products["list_price"].astype(float)))

    # Customer and device dimensions. IDs are synthetic, source user IDs are not required to be PII.
    customer_ids = np.array([f"C{i:07d}" for i in range(1, args.customers + 1)], dtype=object)
    customers = pd.DataFrame({"customer_id": customer_ids, "user_id": [f"user_{i:07d}" for i in range(1, args.customers + 1)]})
    customers.to_csv(out / "01_customers.csv", index=False)
    anonymous_ids = np.array([f"A{i:07d}" for i in range(1, args.devices + 1)], dtype=object)
    device_customer = np.full(args.devices, None, dtype=object)
    linked_n = min(args.customers, args.devices)
    device_customer[:linked_n] = customer_ids[:linked_n]
    # Shuffle which devices are associated with customers.
    rng.shuffle(device_customer)
    device_types = rng.choice(["desktop", "mobile", "tablet"], size=args.devices, p=[0.35, 0.60, 0.05])
    devices = pd.DataFrame({"anonymous_id": anonymous_ids, "customer_id": device_customer, "device_type": device_types})
    devices.to_csv(out / "02_devices.csv", index=False)

    # Session date range is exactly calendar_days, ending at the most recent source event date if parseable.
    latest = pd.Timestamp("2020-02-29")
    for bucket in reservoirs.values():
        if bucket:
            # PATCH: Kaggle's event_time looks like "2019-10-01 00:00:00 UTC" (timezone-aware);
            # comparing it with the naive `latest` above raised TypeError. Parse as UTC, drop tz.
            candidate = pd.to_datetime([r.get("event_time") for r in bucket], errors="coerce", utc=True).tz_localize(None)
            if len(candidate) and candidate.notna().any():
                latest = max(latest, candidate.max().normalize())
    days = pd.date_range(end=latest.normalize(), periods=args.calendar_days, freq="D")
    date_key_by_date = {d.strftime("%Y-%m-%d"): int(d.strftime("%Y%m%d")) for d in days}
    calendar = pd.DataFrame({"date_key": [int(d.strftime("%Y%m%d")) for d in days],
                             "holiday": [False] * len(days),
                             "weekend": [bool(d.dayofweek >= 5) for d in days],
                             "event_flag": [False] * len(days)})
    calendar.to_csv(out / "03_calendar_days.csv", index=False)

    session_device_idx = rng.integers(0, args.devices, size=args.sessions)
    session_anon = anonymous_ids[session_device_idx]
    session_customer = device_customer[session_device_idx]
    session_day_idx = rng.integers(0, len(days), size=args.sessions)
    session_start_seconds = rng.integers(0, 24 * 60 * 60, size=args.sessions)
    session_starts = days.values[session_day_idx].astype("datetime64[ns]") + session_start_seconds.astype("timedelta64[s]")
    session_lengths = rng.integers(30, 60 * 60, size=args.sessions)
    session_ends = session_starts + session_lengths.astype("timedelta64[s]")
    session_ids = np.array([f"S{i:08d}" for i in range(1, args.sessions + 1)], dtype=object)
    channels = rng.choice(["organic_search", "paid_social", "email", "direct", "referral"], size=args.sessions)
    sessions = pd.DataFrame({"session_id": session_ids, "anonymous_id": session_anon,
                             "date_key": [int(pd.Timestamp(x).strftime("%Y%m%d")) for x in session_starts],
                             "channel": channels,
                             "started_at": pd.to_datetime(session_starts).astype(str),
                             "ended_at": pd.to_datetime(session_ends).astype(str)})
    sessions.to_csv(out / "04_sessions.csv", index=False)

    # Target event counts. They sum to --events. Event 2 is cart_remove to preserve Kaggle meaning.
    counts = {
        1: args.cart_items,
        2: min(200_000, max(0, args.events - required_child_events)),
        3: args.checkouts,
        4: 0,
        5: 0,
        6: 0,
        7: args.payment_attempts,
        8: args.orders,
    }
    # Fill remaining events primarily with product views and then session_end rows.
    remaining = args.events - sum(counts.values())
    counts[6] = int(remaining * 0.92)
    counts[4] = remaining - counts[6]
    # Make exactly requested child counts. Event type 2 is not tied to a child table.
    assert sum(counts.values()) == args.events, (counts, sum(counts.values()), args.events)

    event_types = pd.DataFrame(EVENT_TYPES, columns=["event_type_id", "event_type_name", "funnel_order"])
    event_types.to_csv(out / "05_event_types.csv", index=False)

    # Build and shuffle a compact type plan so events are interleaved and counts remain exact.
    type_plan = np.concatenate([np.full(n, type_id, dtype=np.uint8) for type_id, n in counts.items() if n > 0])
    rng.shuffle(type_plan)
    events_path = out / "08_events.csv"
    child_files = {
        1: (out / "09_cart_items.csv", open(out / "09_cart_items.csv", "w", newline="", encoding="utf-8")),
        3: (out / "10_checkouts.csv", open(out / "10_checkouts.csv", "w", newline="", encoding="utf-8")),
        7: (out / "11_payment_attempts.csv", open(out / "11_payment_attempts.csv", "w", newline="", encoding="utf-8")),
        8: (out / "12_orders.csv", open(out / "12_orders.csv", "w", newline="", encoding="utf-8")),
    }
    child_writers = {}
    child_counts = defaultdict(int)
    for type_id, (_, f) in child_files.items():
        w = csv.writer(f)
        child_writers[type_id] = w
        if type_id == 1: w.writerow(["event_id", "quantity", "cart_value"])
        elif type_id == 3: w.writerow(["event_id", "cart_value", "shipping_fee"])
        elif type_id == 7: w.writerow(["event_id", "amount", "status"])
        elif type_id == 8: w.writerow(["order_id", "event_id", "session_id", "customer_id", "product_id", "coupon_code", "order_value"])

    event_columns = ["event_id", "session_id", "event_type_id", "product_id", "coupon_code", "error_code", "event_ts", "price", "latency"]
    order_seq = 0
    try:
        with open(events_path, "w", newline="", encoding="utf-8") as ef:
            ew = csv.DictWriter(ef, fieldnames=event_columns)
            ew.writeheader()
            for start in range(0, args.events, args.batch_size):
                end = min(start + args.batch_size, args.events)
                type_batch = type_plan[start:end]
                batch_rows = []
                for offset, type_id_raw in enumerate(type_batch):
                    type_id = int(type_id_raw)
                    eid = f"E{start + offset + 1:09d}"
                    sess_idx = int(rng.integers(0, args.sessions))
                    sid = session_ids[sess_idx]
                    # Use source data for source event types, and source-derived product/price templates
                    # for simulated checkout/payment/session events.
                    source_name = {1: "cart", 2: "remove_from_cart", 6: "view", 8: "purchase"}.get(type_id, "view")
                    rec = sample_record(reservoirs, source_name, rng)
                    pid = rec.get("product_id")
                    if pid not in valid_product_set:
                        pid = str(valid_products[int(rng.integers(0, len(valid_products)))])
                    price = rec.get("price", np.nan)
                    try: price = float(price)
                    except (TypeError, ValueError): price = np.nan
                    if not np.isfinite(price) or price < 0:
                        price = price_by_product.get(str(pid), 0.0)
                    # Event time is generated within the assigned session.
                    start_dt = pd.Timestamp(session_starts[sess_idx])
                    max_secs = max(1, int(session_lengths[sess_idx]))
                    event_ts = start_dt + pd.Timedelta(seconds=int(rng.integers(0, max_secs)))
                    latency = int(rng.integers(40, 1800)) if type_id in (6, 1, 2, 3) else np.nan
                    coupon = None
                    error = None
                    if type_id == 3 and rng.random() < 0.08:
                        coupon = f"SAVE{int(rng.integers(5, 31))}"
                    if type_id == 7 and rng.random() < 0.12:
                        error = f"PAYMENT_ERR_{int(rng.integers(1, 6))}"
                    row = {"event_id": eid, "session_id": sid, "event_type_id": type_id,
                           "product_id": pid, "coupon_code": coupon, "error_code": error,
                           "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
                           "price": round(price, 2), "latency": latency}
                    batch_rows.append(row)

                    if type_id in child_writers:
                        child_counts[type_id] += 1
                        if type_id == 1:
                            child_writers[type_id].writerow([eid, 1, round(price, 2)])
                        elif type_id == 3:
                            child_writers[type_id].writerow([eid, round(price, 2), round(float(rng.choice([0, 25, 50, 75])), 2)])
                        elif type_id == 7:
                            status = "failed" if error else "succeeded"
                            child_writers[type_id].writerow([eid, round(price, 2), status])
                        elif type_id == 8:
                            order_seq += 1
                            cust = session_customer[sess_idx]
                            child_writers[type_id].writerow([f"O{order_seq:08d}", eid, sid,
                                cust if cust is not None else "", pid, coupon or "", round(price, 2)])
                ew.writerows(batch_rows)
                print(f"Events written: {end:,}/{args.events:,}", end="\r", flush=True)
    finally:
        for _, f in child_files.values():
            f.close()
    print()

    # Mark calendar days with event activity, based on generated session date coverage.
    event_dates = set(pd.to_datetime(session_starts).strftime("%Y-%m-%d"))
    calendar["event_flag"] = [d.strftime("%Y-%m-%d") in event_dates for d in days]
    calendar.to_csv(out / "03_calendar_days.csv", index=False)

    manifest_rows = []
    for path in sorted(out.glob("*.csv")):
        # Count data rows without loading large CSVs into memory.
        with open(path, "r", encoding="utf-8", newline="") as f:
            row_count = sum(1 for _ in f) - 1
        manifest_rows.append({"file": path.name, "rows": row_count})
    pd.DataFrame(manifest_rows).to_csv(out / "manifest.csv", index=False)
    print("\nGeneration complete. CSV row counts:")
    print(pd.DataFrame(manifest_rows).to_string(index=False))
    print(f"\nOutput directory: {out.resolve()}")
    print("Note: checkout_started and payment_attempt events are synthetic; source event rows are sampled with replacement.")

if __name__ == "__main__":
    main()

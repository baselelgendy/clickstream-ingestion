"""Table definitions of the 12-table clickstream model and row casting.

Streamed tables (fact-like, go through Kafka): events + the 4 child tables.
Dimension tables (static, uploaded to S3 once as-is): the other 7.
"""

NULL_TOKENS = {"", "nan", "NaN", "None", "null"}

# table -> (csv file, Kafka message key column, {column: type})
STREAM_TABLES = {
    "events": (
        "08_events.csv",
        "session_id",
        {"event_type_id": int, "price": float, "latency": int},
    ),
    "cart_items": (
        "09_cart_items.csv",
        "event_id",
        {"quantity": int, "cart_value": float},
    ),
    "checkouts": (
        "10_checkouts.csv",
        "event_id",
        {"cart_value": float, "shipping_fee": float},
    ),
    "payment_attempts": (
        "11_payment_attempts.csv",
        "event_id",
        {"amount": float},
    ),
    "orders": (
        "12_orders.csv",
        "event_id",
        {"order_value": float},
    ),
}

# Child tables share event_id with events and are written by the generator in event order.
CHILD_TABLES = ["cart_items", "checkouts", "payment_attempts", "orders"]

DIMENSION_TABLES = {
    "customers": "01_customers.csv",
    "devices": "02_devices.csv",
    "calendar_days": "03_calendar_days.csv",
    "sessions": "04_sessions.csv",
    "event_types": "05_event_types.csv",
    "categories": "06_categories.csv",
    "products": "07_products.csv",
}


def _cast(value, typ):
    if value is None or value in NULL_TOKENS:
        return None
    if typ is int:
        return int(float(value))
    if typ is float:
        return float(value)
    return value


def cast_row(table: str, row: dict) -> dict:
    """CSV strings -> typed JSON values. Empty / 'nan' become null."""
    types = STREAM_TABLES[table][2]
    return {col: _cast(val, types.get(col, str)) for col, val in row.items()}

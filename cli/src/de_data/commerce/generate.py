"""Deterministic, multi-company commerce dataset generator.

``generate(seed, scale, out_dir)`` writes eight tables as both Parquet and CSV and
returns a mapping of table name -> Parquet path. Output is a pure function of
``(seed, scale)``: the same inputs always produce byte-identical table contents.

Design notes
------------
* **Multi-company with an acquisition.** Three companies exist. Company ``C003`` is
  *acquired* by the parent ``C001`` mid-period. Its customer surrogate keys *collide*
  with the parent's (same ``customer_id`` integers refer to different people) and its
  product codes use a different scheme (``X-`` prefix vs. ``P-``), so MDM exercises
  have something real to resolve.
* **Deliberate dirt.** A small, deterministic amount of: NULLs (customer emails,
  some payment amounts), duplicate order-item events, bad currency codes
  (``"XXX"``, lowercase ``"usd"``), and timestamps in mixed time zones (UTC, plus
  ``+02:00`` and ``-05:00`` offsets rendered as ISO strings in a parallel column).
* **Fast at small scale.** ``scale≈0.001`` yields a few hundred rows total.
"""

from __future__ import annotations

import csv
import hashlib
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants (fixed, so output is deterministic across machines)
# ---------------------------------------------------------------------------

PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_DAYS = 180
ACQUISITION_DAY = 90  # C003 becomes part of C001 on this day

CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
# Deliberate dirt: these show up occasionally in the orders feed.
BAD_CURRENCIES = ["XXX", "usd", ""]

TZ_OFFSETS = ["+00:00", "+02:00", "-05:00"]  # mixed time zones in the raw string column

TABLES = [
    "companies",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "fx_rates",
]


def _rng(seed: int, salt: str) -> random.Random:
    """A sub-stream RNG derived from the master seed and a salt label.

    Using a hash of ``seed:salt`` keeps each table's stream independent and stable
    even if we change how many draws another table makes.
    """
    h = hashlib.sha256(f"{seed}:{salt}".encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _scaled(base: int, scale: float, minimum: int = 1) -> int:
    return max(minimum, int(round(base * scale)))


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def _render_in_offset(dt: datetime, offset: str) -> str:
    """Render the same UTC instant as local time in ``offset`` (e.g. '-05:00')."""
    sign = 1 if offset[0] == "+" else -1
    hh, mm = int(offset[1:3]), int(offset[4:6])
    tz = timezone(sign * timedelta(hours=hh, minutes=mm))
    return dt.astimezone(tz).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Table builders. Each returns a list[dict] with stable column order.
# ---------------------------------------------------------------------------


def _companies() -> list[dict]:
    return [
        {"company_id": "C001", "name": "Parent Retail Co", "country": "US",
         "base_currency": "USD", "acquired_by": None, "acquired_on": None},
        {"company_id": "C002", "name": "Euro Goods GmbH", "country": "DE",
         "base_currency": "EUR", "acquired_by": None, "acquired_on": None},
        {"company_id": "C003", "name": "Island Traders Ltd", "country": "GB",
         "base_currency": "GBP", "acquired_by": "C001",
         "acquired_on": _iso(PERIOD_START + timedelta(days=ACQUISITION_DAY))},
    ]


def _customers(seed: int, scale: float) -> list[dict]:
    rng = _rng(seed, "customers")
    rows: list[dict] = []
    # Parent + euro company get fresh id spaces; acquired company REUSES C001 ids
    # (deliberate key collision for MDM).
    per_company = {
        "C001": _scaled(40, scale, 4),
        "C002": _scaled(25, scale, 3),
        "C003": _scaled(15, scale, 2),
    }
    first = ["Ava", "Liam", "Noor", "Mateo", "Yuki", "Omar", "Elena", "Sven",
             "Priya", "Hans", "Chloe", "Diego"]
    last = ["Khan", "Smith", "Mueller", "Rossi", "Tanaka", "Dubois", "Novak",
            "Garcia", "Olsen", "Patel"]
    for company, n in per_company.items():
        for i in range(1, n + 1):
            # C003 ids intentionally overlap C001's 1..n range.
            cust_id = i
            email = f"user{i}@{company.lower()}.example"
            # Dirt: some emails are NULL.
            if rng.random() < 0.08:
                email = None
            rows.append({
                "company_id": company,
                "customer_id": cust_id,
                "first_name": rng.choice(first),
                "last_name": rng.choice(last),
                "email": email,
                "signup_date": _iso(PERIOD_START + timedelta(
                    days=rng.randint(0, PERIOD_DAYS - 1))),
            })
    return rows


def _products(seed: int, scale: float) -> list[dict]:
    rng = _rng(seed, "products")
    rows: list[dict] = []
    catalog = {
        "C001": ("P-", _scaled(20, scale, 3)),
        "C002": ("P-", _scaled(15, scale, 3)),
        "C003": ("X-", _scaled(12, scale, 2)),  # different code scheme
    }
    names = ["Widget", "Gadget", "Sprocket", "Cog", "Bolt", "Panel", "Module",
             "Sensor", "Cable", "Frame"]
    for company, (prefix, n) in catalog.items():
        for i in range(1, n + 1):
            rows.append({
                "company_id": company,
                "product_code": f"{prefix}{i:04d}",
                "name": f"{rng.choice(names)} {i}",
                "unit_price": round(rng.uniform(5, 500), 2),
                "currency": {"C001": "USD", "C002": "EUR", "C003": "GBP"}[company],
            })
    return rows


def _orders(seed: int, scale: float, customers: list[dict]) -> list[dict]:
    rng = _rng(seed, "orders")
    rows: list[dict] = []
    by_company: dict[str, list[int]] = {}
    for c in customers:
        by_company.setdefault(c["company_id"], []).append(c["customer_id"])

    n_orders = _scaled(200, scale, 10)
    companies = list(by_company.keys())
    for oid in range(1, n_orders + 1):
        company = rng.choice(companies)
        cust = rng.choice(by_company[company])
        day = rng.randint(0, PERIOD_DAYS - 1)
        ts = PERIOD_START + timedelta(days=day, seconds=rng.randint(0, 86399))
        # Mixed-timezone raw string column (dirt): same instant, different offset text.
        offset = rng.choice(TZ_OFFSETS)
        # Dirt: occasional bad currency code.
        if rng.random() < 0.05:
            currency = rng.choice(BAD_CURRENCIES)
        else:
            currency = {"C001": "USD", "C002": "EUR", "C003": "GBP"}[company]
        rows.append({
            "company_id": company,
            "order_id": oid,
            "customer_id": cust,
            "order_ts_utc": _iso(ts),
            # Same instant rendered in a different local offset (e.g. 2026-05-09T23:04:40+02:00).
            "order_ts_raw": _render_in_offset(ts, offset),
            "currency": currency,
            # acquired company's orders after the acquisition carry parent flag
            "under_parent": bool(company == "C003" and day >= ACQUISITION_DAY),
        })
    return rows


def _order_items(seed: int, scale: float, orders: list[dict],
                 products: list[dict]) -> list[dict]:
    rng = _rng(seed, "order_items")
    prod_by_company: dict[str, list[str]] = {}
    for p in products:
        prod_by_company.setdefault(p["company_id"], []).append(p["product_code"])

    rows: list[dict] = []
    line_id = 0
    for o in orders:
        codes = prod_by_company.get(o["company_id"], [])
        if not codes:
            continue
        n_lines = rng.randint(1, 3)
        for _ in range(n_lines):
            line_id += 1
            row = {
                "company_id": o["company_id"],
                "order_item_id": line_id,
                "order_id": o["order_id"],
                "product_code": rng.choice(codes),
                "qty": rng.randint(1, 5),
                "line_amount": round(rng.uniform(5, 1000), 2),
            }
            rows.append(row)
            # Dirt: duplicate order-item events (same id and payload) now and then.
            if rng.random() < 0.04:
                rows.append(dict(row))
    return rows


def _payments(seed: int, scale: float, orders: list[dict]) -> list[dict]:
    rng = _rng(seed, "payments")
    rows: list[dict] = []
    pid = 0
    for o in orders:
        # Most orders have a payment; a few do not (dirt via missing rows).
        if rng.random() < 0.1:
            continue
        pid += 1
        amount = round(rng.uniform(10, 2000), 2)
        # Dirt: some payment amounts are NULL.
        if rng.random() < 0.06:
            amount = None
        rows.append({
            "company_id": o["company_id"],
            "payment_id": pid,
            "order_id": o["order_id"],
            "amount": amount,
            "currency": o["currency"],
            "paid_ts_utc": o["order_ts_utc"],
        })
    return rows


def _refunds(seed: int, scale: float, payments: list[dict]) -> list[dict]:
    rng = _rng(seed, "refunds")
    rows: list[dict] = []
    rid = 0
    for p in payments:
        if rng.random() < 0.1 and p["amount"] is not None:
            rid += 1
            rows.append({
                "company_id": p["company_id"],
                "refund_id": rid,
                "payment_id": p["payment_id"],
                "amount": round(p["amount"] * rng.uniform(0.1, 1.0), 2),
                "currency": p["currency"],
                "refund_ts_utc": p["paid_ts_utc"],
            })
    return rows


def _fx_rates(seed: int) -> list[dict]:
    rng = _rng(seed, "fx")
    rows: list[dict] = []
    # Daily USD-base rates for the period. Deterministic random walk.
    rates = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067}
    for day in range(PERIOD_DAYS):
        date = (PERIOD_START + timedelta(days=day)).date().isoformat()
        for ccy, base in rates.items():
            drift = 1.0 + (rng.random() - 0.5) * 0.02
            rows.append({
                "rate_date": date,
                "currency": ccy,
                "usd_per_unit": round(base * drift, 6),
            })
    return rows


# ---------------------------------------------------------------------------
# Arrow schemas (explicit, so Parquet types are stable regardless of NULLs)
# ---------------------------------------------------------------------------

_SCHEMAS: dict[str, pa.Schema] = {
    "companies": pa.schema([
        ("company_id", pa.string()), ("name", pa.string()), ("country", pa.string()),
        ("base_currency", pa.string()), ("acquired_by", pa.string()),
        ("acquired_on", pa.string()),
    ]),
    "customers": pa.schema([
        ("company_id", pa.string()), ("customer_id", pa.int64()),
        ("first_name", pa.string()), ("last_name", pa.string()),
        ("email", pa.string()), ("signup_date", pa.string()),
    ]),
    "products": pa.schema([
        ("company_id", pa.string()), ("product_code", pa.string()),
        ("name", pa.string()), ("unit_price", pa.float64()),
        ("currency", pa.string()),
    ]),
    "orders": pa.schema([
        ("company_id", pa.string()), ("order_id", pa.int64()),
        ("customer_id", pa.int64()), ("order_ts_utc", pa.string()),
        ("order_ts_raw", pa.string()), ("currency", pa.string()),
        ("under_parent", pa.bool_()),
    ]),
    "order_items": pa.schema([
        ("company_id", pa.string()), ("order_item_id", pa.int64()),
        ("order_id", pa.int64()), ("product_code", pa.string()),
        ("qty", pa.int64()), ("line_amount", pa.float64()),
    ]),
    "payments": pa.schema([
        ("company_id", pa.string()), ("payment_id", pa.int64()),
        ("order_id", pa.int64()), ("amount", pa.float64()),
        ("currency", pa.string()), ("paid_ts_utc", pa.string()),
    ]),
    "refunds": pa.schema([
        ("company_id", pa.string()), ("refund_id", pa.int64()),
        ("payment_id", pa.int64()), ("amount", pa.float64()),
        ("currency", pa.string()), ("refund_ts_utc", pa.string()),
    ]),
    "fx_rates": pa.schema([
        ("rate_date", pa.string()), ("currency", pa.string()),
        ("usd_per_unit", pa.float64()),
    ]),
}


def _write_table(name: str, rows: list[dict], out_dir: Path) -> Path:
    schema = _SCHEMAS[name]
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    table = pa.Table.from_pydict(cols, schema=schema)

    pq_path = out_dir / f"{name}.parquet"
    # Fixed compression + no stats timestamps keeps bytes stable for a given input.
    pq.write_table(table, pq_path, compression="snappy")

    csv_path = out_dir / f"{name}.csv"
    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[f.name for f in schema])
        writer.writeheader()
        for r in rows:
            writer.writerow({k: ("" if r.get(k) is None else r.get(k))
                             for k in writer.fieldnames})
    return pq_path


def generate(seed: int, scale: float, out_dir) -> dict[str, Path]:
    """Generate the multi-company commerce dataset.

    Parameters
    ----------
    seed:
        Master seed. Output is a deterministic function of ``(seed, scale)``.
    scale:
        Approximate size. ``1.0`` ~ 1 GB; tests use ``~0.001``.
    out_dir:
        Directory to write into (created if missing).

    Returns
    -------
    dict[str, Path]
        Mapping of table name -> Parquet path.
    """
    if scale <= 0:
        raise ValueError(f"scale must be > 0, got {scale!r}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    companies = _companies()
    customers = _customers(seed, scale)
    products = _products(seed, scale)
    orders = _orders(seed, scale, customers)
    order_items = _order_items(seed, scale, orders, products)
    payments = _payments(seed, scale, orders)
    refunds = _refunds(seed, scale, payments)
    fx_rates = _fx_rates(seed)

    data = {
        "companies": companies,
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "refunds": refunds,
        "fx_rates": fx_rates,
    }

    paths: dict[str, Path] = {}
    for name in TABLES:
        paths[name] = _write_table(name, data[name], out)
    return paths

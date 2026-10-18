"""de_data.commerce.generate: determinism + required properties."""

import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from de_data.commerce import generate
from de_data.commerce.generate import TABLES


def _digest(paths):
    h = hashlib.sha256()
    for name in sorted(paths):
        h.update(name.encode())
        h.update(Path(paths[name]).read_bytes())
    return h.hexdigest()


def test_all_tables_written(tmp_path):
    paths = generate(seed=1, scale=0.001, out_dir=tmp_path / "a")
    assert set(paths) == set(TABLES)
    for name, p in paths.items():
        assert p.exists(), name
        assert p.with_suffix(".csv").exists(), f"{name} csv"


def test_deterministic_same_seed(tmp_path):
    p1 = generate(seed=7, scale=0.001, out_dir=tmp_path / "a")
    p2 = generate(seed=7, scale=0.001, out_dir=tmp_path / "b")
    assert _digest(p1) == _digest(p2)


def test_different_seed_differs(tmp_path):
    p1 = generate(seed=7, scale=0.001, out_dir=tmp_path / "a")
    p2 = generate(seed=8, scale=0.001, out_dir=tmp_path / "b")
    assert _digest(p1) != _digest(p2)


def test_multi_company_with_acquisition(tmp_path):
    paths = generate(seed=3, scale=0.001, out_dir=tmp_path)
    companies = pq.read_table(paths["companies"]).to_pylist()
    ids = {c["company_id"] for c in companies}
    assert {"C001", "C002", "C003"} <= ids
    acquired = [c for c in companies if c["acquired_by"]]
    assert acquired and acquired[0]["company_id"] == "C003"
    assert acquired[0]["acquired_by"] == "C001"


def test_key_collision_for_mdm(tmp_path):
    paths = generate(seed=3, scale=0.001, out_dir=tmp_path)
    custs = pq.read_table(paths["customers"]).to_pylist()
    c001_ids = {c["customer_id"] for c in custs if c["company_id"] == "C001"}
    c003_ids = {c["customer_id"] for c in custs if c["company_id"] == "C003"}
    # Acquired company reuses the parent's id space (collision).
    assert c003_ids & c001_ids, "expected colliding customer ids across companies"


def test_product_code_schemes_differ(tmp_path):
    paths = generate(seed=3, scale=0.001, out_dir=tmp_path)
    prods = pq.read_table(paths["products"]).to_pylist()
    c001 = [p["product_code"] for p in prods if p["company_id"] == "C001"]
    c003 = [p["product_code"] for p in prods if p["company_id"] == "C003"]
    assert all(code.startswith("P-") for code in c001)
    assert all(code.startswith("X-") for code in c003)


def test_deliberate_dirt_present(tmp_path):
    # Use a slightly larger scale so the small-probability dirt reliably appears.
    paths = generate(seed=11, scale=0.05, out_dir=tmp_path)
    orders = pq.read_table(paths["orders"]).to_pylist()
    customers = pq.read_table(paths["customers"]).to_pylist()
    order_items = pq.read_table(paths["order_items"]).to_pylist()

    # bad currency codes
    bad = {"XXX", "usd", ""}
    assert any(o["currency"] in bad for o in orders), "expected bad currency codes"
    # null emails
    assert any(c["email"] is None for c in customers), "expected null emails"
    # mixed timezone raw strings
    offsets = {o["order_ts_raw"][-6:] for o in orders}
    assert len({x for x in offsets if x in ("+00:00", "+02:00", "-05:00")}) >= 2
    # raw string is the SAME instant as order_ts_utc, just rendered in another offset
    from datetime import datetime
    for o in orders:
        assert datetime.fromisoformat(o["order_ts_raw"]) == datetime.fromisoformat(o["order_ts_utc"])
    # duplicate order-item events
    keys = [tuple(sorted(oi.items())) for oi in order_items]
    assert len(keys) != len(set(keys)), "expected duplicate order-item events"

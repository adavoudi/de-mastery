"""Tests for ZZZ-01/01-dedup-orders. Run via the `submission` fixture.

Oracle: DuckDB computes the expected cleaned set independently (G6 spirit).
"""

VALID = ("USD", "EUR", "GBP", "JPY")


def _rows():
    return [
        {"order_item_id": 1, "order_id": 10, "product_code": "P-0001",
         "qty": 2, "currency": "USD"},
        # exact duplicate of row 1 -> must be removed
        {"order_item_id": 1, "order_id": 10, "product_code": "P-0001",
         "qty": 2, "currency": "USD"},
        {"order_item_id": 2, "order_id": 10, "product_code": "P-0002",
         "qty": 1, "currency": "EUR"},
        # same id as row with id=1 but different payload -> must be KEPT
        {"order_item_id": 1, "order_id": 11, "product_code": "P-0009",
         "qty": 5, "currency": "GBP"},
        # bad currency -> must be dropped
        {"order_item_id": 3, "order_id": 12, "product_code": "P-0003",
         "qty": 1, "currency": "XXX"},
        # lowercase currency is invalid -> dropped
        {"order_item_id": 4, "order_id": 12, "product_code": "P-0004",
         "qty": 1, "currency": "usd"},
    ]


def _oracle(con, rows):
    import pyarrow as pa  # noqa: F401
    con.execute("""
        create table r(order_item_id int, order_id int, product_code varchar,
                       qty int, currency varchar)
    """)
    con.executemany(
        "insert into r values (?,?,?,?,?)",
        [(x["order_item_id"], x["order_id"], x["product_code"], x["qty"],
          x["currency"]) for x in rows],
    )
    # distinct full rows with valid currency
    res = con.execute("""
        select distinct order_item_id, order_id, product_code, qty, currency
        from r
        where currency in ('USD','EUR','GBP','JPY')
    """).fetchall()
    return {tuple(t) for t in res}


def _as_set(rows):
    return {(r["order_item_id"], r["order_id"], r["product_code"], r["qty"],
             r["currency"]) for r in rows}


def test_matches_oracle(submission, duckdb_con):
    rows = _rows()
    out = submission.clean_orders([dict(r) for r in rows])
    expected = _oracle(duckdb_con, rows)
    assert _as_set(out) == expected


def test_removes_exact_duplicates(submission):
    rows = _rows()
    out = submission.clean_orders([dict(r) for r in rows])
    # No exact duplicate rows remain.
    seen = set()
    for r in out:
        key = (r["order_item_id"], r["order_id"], r["product_code"], r["qty"],
               r["currency"])
        assert key not in seen, f"duplicate row survived: {r}"
        seen.add(key)


def test_keeps_distinct_rows_sharing_an_id(submission):
    rows = _rows()
    out = submission.clean_orders([dict(r) for r in rows])
    ids_for_1 = sorted(r["order_id"] for r in out if r["order_item_id"] == 1)
    # id=1 appears for order 10 (once) and order 11 (distinct payload) -> both kept
    assert ids_for_1 == [10, 11], f"got {ids_for_1}"


def test_drops_invalid_currency(submission):
    rows = _rows()
    out = submission.clean_orders([dict(r) for r in rows])
    assert all(r["currency"] in VALID for r in out)
    assert not any(r["currency"] in ("XXX", "usd") for r in out)


def test_preserves_order(submission):
    rows = _rows()
    out = submission.clean_orders([dict(r) for r in rows])
    # First kept row must be the first valid, non-duplicate input row.
    assert out[0]["order_id"] == 10 and out[0]["order_item_id"] == 1

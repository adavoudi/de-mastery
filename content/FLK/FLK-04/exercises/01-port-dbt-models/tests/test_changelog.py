"""FLK-04/01 — port three dbt models to Flink SQL, graded on the CHANGELOG.

The learner writes three SQL files under ``queries/`` that read a table ``orders``
(the test provides the DDL as a VALUES-backed temporary view):

  * ``stg_valid_orders.sql``  — a filter/project. APPEND-ONLY (+I only).
  * ``count_per_customer.sql`` — COUNT(*) per customer. UPDATING (-U/+U).
  * ``gmv_per_company.sql``    — SUM(amount) per company. UPDATING (-U/+U).

We assert BOTH the changelog shape (which RowKinds appear) AND that the folded
final state equals an INDEPENDENT DuckDB batch answer over the same rows (G6).
This catches queries that are the wrong KIND (append where it should update, or
vice-versa) even if a single snapshot looks right.

SKIPs cleanly when the Flink dist / JDK are absent (via the flink_sql fixture).
"""

from pathlib import Path

# the orders fixture: order_id, company_id, customer_id, currency, amount.
# cust1 has 3 orders (o1,o3,o4) incl. one invalid currency; cust2 has 1.
# company c1 total amount = 100 + 70 + 20 = 190; c2 = 50.
ORDERS = [
    ("o1", "c1", "cust1", "USD", 100),
    ("o2", "c2", "cust2", "EUR", 50),
    ("o3", "c1", "cust1", "ZZZ", 70),
    ("o4", "c1", "cust1", "USD", 20),
]
VALID = {"USD", "EUR", "GBP", "JPY"}


def _orders_ddl():
    values = ",".join(
        f"('{oid}','{co}','{cu}','{cur}',{amt})"
        for oid, co, cu, cur, amt in ORDERS)
    return [
        "CREATE TEMPORARY VIEW orders AS SELECT * FROM (VALUES " + values
        + ") AS t(order_id, company_id, customer_id, currency, amount)"
    ]


def _read(submission_dir, name):
    p = Path(submission_dir) / "queries" / name
    assert p.exists(), f"missing {p}"
    return p.read_text().strip().rstrip(";")


def _fold(rows, key_idx=1, val_idx=2):
    """Fold a changelog into the final per-key state (last +I/+U wins; -D removes)."""
    state = {}
    for r in rows:
        kind = r[0]
        key = r[key_idx]
        if kind in ("+I", "+U"):
            state[key] = r[val_idx]
        elif kind == "-D":
            state.pop(key, None)
    return state


# ----- DuckDB oracles (G6) --------------------------------------------------
def _oracle_valid_ids(con):
    con.execute("CREATE OR REPLACE TABLE o(order_id VARCHAR, company_id VARCHAR, "
                "customer_id VARCHAR, currency VARCHAR, amount INT)")
    con.executemany("INSERT INTO o VALUES (?,?,?,?,?)", ORDERS)
    rows = con.execute(
        "SELECT order_id FROM o "
        "WHERE currency IN ('USD','EUR','GBP','JPY')").fetchall()
    return {r[0] for r in rows}


def _oracle_count(con):
    con.execute("CREATE OR REPLACE TABLE o(order_id VARCHAR, company_id VARCHAR, "
                "customer_id VARCHAR, currency VARCHAR, amount INT)")
    con.executemany("INSERT INTO o VALUES (?,?,?,?,?)", ORDERS)
    rows = con.execute(
        "SELECT customer_id, COUNT(*) FROM o GROUP BY customer_id").fetchall()
    return {r[0]: str(r[1]) for r in rows}


def _oracle_gmv(con):
    con.execute("CREATE OR REPLACE TABLE o(order_id VARCHAR, company_id VARCHAR, "
                "customer_id VARCHAR, currency VARCHAR, amount INT)")
    con.executemany("INSERT INTO o VALUES (?,?,?,?,?)", ORDERS)
    rows = con.execute(
        "SELECT company_id, SUM(amount) FROM o GROUP BY company_id").fetchall()
    return {r[0]: str(r[1]) for r in rows}


# ----- tests ----------------------------------------------------------------
def test_stg_valid_orders_is_append_only(flink_sql, submission_dir, duckdb_con):
    sql = _read(submission_dir, "stg_valid_orders.sql")
    rows = flink_sql.run(ddl=_orders_ddl(), query=sql, mode="streaming", limit=200)
    assert rows, "no changelog rows"
    kinds = {r[0] for r in rows}
    # a filter/project must be APPEND-ONLY: only +I, no retractions.
    assert kinds == {"+I"}, f"expected append-only (+I), got kinds {kinds}"
    # the first projected column is order_id; compare the valid-order id set to the
    # oracle (projection width is up to the learner, so we key on order_id).
    got_ids = {r[1] for r in rows}
    exp_ids = _oracle_valid_ids(duckdb_con)
    assert got_ids == exp_ids, f"\n got  {sorted(got_ids)}\n want {sorted(exp_ids)}"
    # and no emitted row may carry an invalid currency anywhere in its fields.
    for r in rows:
        assert "ZZZ" not in r[1:], f"invalid-currency row leaked: {r}"


def test_count_per_customer_updates_and_converges(flink_sql, submission_dir, duckdb_con):
    sql = _read(submission_dir, "count_per_customer.sql")
    rows = flink_sql.run(ddl=_orders_ddl(), query=sql, mode="streaming", limit=200)
    assert rows, "no changelog rows"
    kinds = {r[0] for r in rows}
    # an unbounded GROUP BY must be an UPDATING query (it retracts/re-emits).
    assert {"-U", "+U"} <= kinds, f"expected updating (-U/+U), got {kinds}"
    final = _fold(rows)
    assert final == _oracle_count(duckdb_con), f"\n got {final}"


def test_gmv_per_company_updates_and_converges(flink_sql, submission_dir, duckdb_con):
    sql = _read(submission_dir, "gmv_per_company.sql")
    rows = flink_sql.run(ddl=_orders_ddl(), query=sql, mode="streaming", limit=200)
    assert rows, "no changelog rows"
    kinds = {r[0] for r in rows}
    assert {"-U", "+U"} <= kinds, f"expected updating (-U/+U), got {kinds}"
    final = _fold(rows)
    assert final == _oracle_gmv(duckdb_con), f"\n got {final}"


def test_gmv_excludes_nothing_and_counts_all_orders(flink_sql, submission_dir, duckdb_con):
    # GMV is over ALL orders (the dbt model does not filter currency here); c1
    # must be 190 (100+70+20), proving the model didn't accidentally filter.
    sql = _read(submission_dir, "gmv_per_company.sql")
    rows = flink_sql.run(ddl=_orders_ddl(), query=sql, mode="streaming", limit=200)
    final = _fold(rows)
    assert final.get("c1") == "190", final

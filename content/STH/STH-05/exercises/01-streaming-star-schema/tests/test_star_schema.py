"""STH-05 — streaming star schema: SCD2 dim_customer + fct_orders (as-of join),
reconciled to an independent DuckDB batch rebuild (TESTING.md G6).

No Docker: the embedded Flink SQL `paimon_sql` runner materialises the learner's
dim/fct SELECTs into isolated tmp Paimon tables; we read them back in batch and
compare, order-insensitively, against DuckDB (SCD2 window + ASOF join over the
same events). Deterministic, bounded, with explicit per-test timeouts.
"""

import duckdb
import pytest

SENTINEL = 9223372036854775807

# --- deterministic inputs; boundaries chosen so the as-of rule actually bites --
# customer change feed: (cid, eff, name, tier)
CUST = [
    (1, 0, "Alice", "std"),
    (1, 100, "Alice", "gold"),
    (2, 0, "Bob", "std"),
    (2, 50, "Bob", "plat"),
]
# orders: (oid, cid, amt, ots)
ORDERS = [
    (10, 1, 50, 0),     # exactly at valid_from of Alice's first version -> std
    (11, 1, 70, 30),    # inside std -> std
    (12, 1, 90, 100),   # exactly on the std->gold boundary -> gold (half-open)
    (13, 1, 20, 250),   # after -> gold (open interval)
    (14, 2, 20, 10),    # Bob std
    (15, 2, 40, 50),    # exactly on Bob's std->plat boundary -> plat
]


def _oracle_dim():
    con = duckdb.connect()
    con.execute("CREATE TABLE c(cid INT, eff BIGINT, name VARCHAR, tier VARCHAR)")
    con.executemany("INSERT INTO c VALUES (?,?,?,?)", CUST)
    rows = con.execute(f"""
        SELECT cid, name, tier, eff AS valid_from,
          COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) AS valid_to,
          (COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) = {SENTINEL})
            AS is_current
        FROM c
    """).fetchall()
    # Flink's SQL harness stringifies BOOLEAN as 'true'/'false' (lowercase);
    # match that so the order-insensitive oracle compares like-for-like.
    return [(cid, name, tier, vf, vt, "true" if cur else "false")
            for cid, name, tier, vf, vt, cur in rows]


def _oracle_fct():
    con = duckdb.connect()
    con.execute("CREATE TABLE c(cid INT, eff BIGINT, name VARCHAR, tier VARCHAR)")
    con.executemany("INSERT INTO c VALUES (?,?,?,?)", CUST)
    con.execute("CREATE TABLE o(oid INT, cid INT, amt INT, ots BIGINT)")
    con.executemany("INSERT INTO o VALUES (?,?,?,?)", ORDERS)
    # ASOF: greatest eff <= ots (half-open as-of), per customer
    rows = con.execute("""
        SELECT o.oid, o.cid, o.amt, d.tier
        FROM o ASOF JOIN (SELECT cid, eff, tier FROM c) d
          ON o.cid = d.cid AND o.ots >= d.eff
    """).fetchall()
    return rows


# ---- source + target DDL (fixed; the learner owns only the transform SQL) ----
_CUST_DDL = ("CREATE TABLE cust_cdc (cid INT, eff BIGINT, name STRING, tier STRING, "
             "PRIMARY KEY (cid, eff) NOT ENFORCED) WITH ('bucket'='1')")
_ORD_DDL = ("CREATE TABLE orders (oid INT, cid INT, amt INT, ots BIGINT, "
            "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")
_DIM_DDL = ("CREATE TABLE dim_customer (cid INT, name STRING, tier STRING, "
            "valid_from BIGINT, valid_to BIGINT, is_current BOOLEAN, "
            "PRIMARY KEY (cid, valid_from) NOT ENFORCED) WITH ('bucket'='1')")
_FCT_DDL = ("CREATE TABLE fct_orders (oid INT, cid INT, amt INT, tier STRING, "
            "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")


def _cust_insert():
    vals = ",".join(f"({c},{e},'{n}','{t}')" for c, e, n, t in CUST)
    return f"INSERT INTO cust_cdc VALUES {vals}"


def _ord_insert():
    vals = ",".join(f"({o},{c},{a},{t})" for o, c, a, t in ORDERS)
    return f"INSERT INTO orders VALUES {vals}"


def _build_dim(paimon_sql, submission):
    paimon_sql.batch(ddl=[_CUST_DDL, _DIM_DDL], inserts=[_cust_insert()])
    paimon_sql.batch(
        inserts=[f"INSERT INTO dim_customer {submission.dim_scd2_select('cust_cdc')}"])


def _build_fct(paimon_sql, submission):
    paimon_sql.batch(ddl=[_ORD_DDL, _FCT_DDL], inserts=[_ord_insert()])
    paimon_sql.batch(inserts=[
        f"INSERT INTO fct_orders "
        f"{submission.fct_orders_select('orders', 'dim_customer')}"])


# ---------------------------------------------------------------------------
@pytest.mark.timeout(240)
def test_scd2_dim_reconciles(paimon_sql, submission, sth_oracle):
    _build_dim(paimon_sql, submission)
    rows = paimon_sql.batch(
        query="SELECT cid, name, tier, valid_from, valid_to, is_current "
              "FROM dim_customer")
    sth_oracle(rows, _oracle_dim())


@pytest.mark.timeout(300)
def test_fct_orders_as_of_reconciles(paimon_sql, submission, sth_oracle):
    _build_dim(paimon_sql, submission)
    _build_fct(paimon_sql, submission)
    rows = paimon_sql.batch(query="SELECT oid, cid, amt, tier FROM fct_orders")
    sth_oracle(rows, _oracle_fct())


@pytest.mark.timeout(300)
def test_fct_row_count_matches_orders(paimon_sql, submission):
    # completeness: an as-of join must value EVERY order exactly once (no fan-out,
    # no drops). Kills open-valid-to (fan-out) and boundary (drops ots=0).
    _build_dim(paimon_sql, submission)
    _build_fct(paimon_sql, submission)
    rows = paimon_sql.batch(query="SELECT oid FROM fct_orders")
    oids = [r[1] for r in rows]
    assert sorted(oids) == sorted(str(o[0]) for o in ORDERS), \
        f"every order must appear exactly once; got {sorted(oids)}"


@pytest.mark.timeout(240)
def test_boundary_order_takes_new_version(paimon_sql, submission):
    # order 12 (ots=100) is exactly on Alice's std->gold boundary: as-of => gold.
    _build_dim(paimon_sql, submission)
    _build_fct(paimon_sql, submission)
    rows = paimon_sql.batch(
        query="SELECT oid, tier FROM fct_orders WHERE oid = 12")
    assert rows and rows[0][2] == "gold", \
        f"boundary order must take the new (gold) version, got {rows}"

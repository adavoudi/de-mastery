"""STH-06 — in-flight DQ + quarantine/DLQ on the streaming orders pipeline.

The acceptance gate is COMPLETENESS: count(good_orders)+count(quarantine) ==
count(orders_raw), with disjoint oid sets — this kills silent-drop mutants.
Good orders and the quarantine (with reasons) also reconcile against an
independent DuckDB computation (G6), and fct_orders built on the good orders
matches the DuckDB as-of join over only the valid orders.

No Docker: embedded Flink SQL (`paimon_sql`). Deterministic, bounded, timed.
"""

import duckdb
import pytest

SENTINEL = 9223372036854775807

# customers that exist
CUSTOMERS = [1, 2]
# customer change feed for the SCD2 dim (cid, eff, name, tier)
CUST_CDC = [
    (1, 0, "Alice", "std"),
    (1, 100, "Alice", "gold"),
    (2, 0, "Bob", "std"),
]
# raw orders (oid, cid, amt, ots); cid may be NULL (-> sentinel below)
#   good: 10 (ok), 15 (ok, boundary gold)
#   bad : 11 non_positive, 12 null_customer, 13 unknown_customer, 14 non_positive
ORDERS_RAW = [
    (10, 1, 50, 30),
    (11, 2, -5, 40),
    (12, None, 20, 10),
    (13, 9, 30, 20),
    (14, 2, 0, 60),
    (15, 1, 90, 150),
]


def _oracle_good():
    con = _con()
    return con.execute(
        "SELECT o.oid, o.cid, o.amt, o.ots FROM orders_raw o "
        "JOIN customers c ON o.cid=c.cid "
        "WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt>0").fetchall()


def _oracle_quarantine():
    con = _con()
    return con.execute("""
        SELECT o.oid,
          CASE WHEN o.cid IS NULL THEN 'null_customer'
               WHEN o.amt IS NULL OR o.amt<=0 THEN 'non_positive_amount'
               WHEN c.cid IS NULL THEN 'unknown_customer' END AS reason
        FROM orders_raw o LEFT JOIN customers c ON o.cid=c.cid
        WHERE NOT (o.cid IS NOT NULL AND o.amt IS NOT NULL
                   AND o.amt>0 AND c.cid IS NOT NULL)
    """).fetchall()


def _oracle_fct():
    con = _con()
    return con.execute("""
        SELECT g.oid, g.cid, g.amt, d.tier
        FROM (
          SELECT o.oid,o.cid,o.amt,o.ots FROM orders_raw o
          JOIN customers c ON o.cid=c.cid
          WHERE o.cid IS NOT NULL AND o.amt IS NOT NULL AND o.amt>0
        ) g
        ASOF JOIN (SELECT cid,eff,tier FROM cust_cdc) d
          ON g.cid=d.cid AND g.ots>=d.eff
    """).fetchall()


def _con():
    con = duckdb.connect()
    con.execute("CREATE TABLE customers(cid INT)")
    con.executemany("INSERT INTO customers VALUES (?)", [(c,) for c in CUSTOMERS])
    con.execute("CREATE TABLE cust_cdc(cid INT,eff BIGINT,name VARCHAR,tier VARCHAR)")
    con.executemany("INSERT INTO cust_cdc VALUES (?,?,?,?)", CUST_CDC)
    con.execute("CREATE TABLE orders_raw(oid INT,cid INT,amt INT,ots BIGINT)")
    con.executemany("INSERT INTO orders_raw VALUES (?,?,?,?)", ORDERS_RAW)
    return con


# ---- fixed source + target DDL (learner owns only the DQ split SQL) ----------
_CUST_DDL = ("CREATE TABLE customers (cid INT, PRIMARY KEY (cid) NOT ENFORCED) "
             "WITH ('bucket'='1')")
_RAW_DDL = ("CREATE TABLE orders_raw (oid INT, cid INT, amt INT, ots BIGINT, "
            "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")
_GOOD_DDL = ("CREATE TABLE good_orders (oid INT, cid INT, amt INT, ots BIGINT, "
             "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")
_QUAR_DDL = ("CREATE TABLE quarantine (oid INT, reason STRING, "
             "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")
_CDC_DDL = ("CREATE TABLE cust_cdc (cid INT, eff BIGINT, name STRING, tier STRING, "
            "PRIMARY KEY (cid, eff) NOT ENFORCED) WITH ('bucket'='1')")
_DIM_DDL = ("CREATE TABLE dim_customer (cid INT, tier STRING, valid_from BIGINT, "
            "valid_to BIGINT, PRIMARY KEY (cid, valid_from) NOT ENFORCED) "
            "WITH ('bucket'='1')")
_FCT_DDL = ("CREATE TABLE fct_orders (oid INT, cid INT, amt INT, tier STRING, "
            "PRIMARY KEY (oid) NOT ENFORCED) WITH ('bucket'='1')")


def _lit(v):
    return "CAST(NULL AS INT)" if v is None else str(v)


def _raw_insert():
    vals = ",".join(f"({o},{_lit(c)},{_lit(a)},{t})" for o, c, a, t in ORDERS_RAW)
    return f"INSERT INTO orders_raw VALUES {vals}"


def _cust_insert():
    return "INSERT INTO customers VALUES " + ",".join(f"({c})" for c in CUSTOMERS)


def _build_dq(paimon_sql, submission):
    paimon_sql.batch(ddl=[_CUST_DDL, _RAW_DDL, _GOOD_DDL, _QUAR_DDL],
                     inserts=[_cust_insert(), _raw_insert()])
    paimon_sql.batch(inserts=[
        f"INSERT INTO good_orders "
        f"{submission.good_orders_select('orders_raw', 'customers')}"])
    paimon_sql.batch(inserts=[
        f"INSERT INTO quarantine "
        f"{submission.quarantine_select('orders_raw', 'customers')}"])


# ---------------------------------------------------------------------------
@pytest.mark.timeout(300)
def test_completeness_no_silent_drop(paimon_sql, submission):
    _build_dq(paimon_sql, submission)
    good = {r[1] for r in paimon_sql.batch(query="SELECT oid FROM good_orders")}
    quar = {r[1] for r in paimon_sql.batch(query="SELECT oid FROM quarantine")}
    inp = {str(o[0]) for o in ORDERS_RAW}
    assert good.isdisjoint(quar), f"good and quarantine overlap: {good & quar}"
    assert good | quar == inp, (
        "silent drop: every input order must be in good_orders OR quarantine. "
        f"missing={inp - (good | quar)} extra={(good | quar) - inp}")
    assert len(good) + len(quar) == len(ORDERS_RAW), \
        "count(good)+count(quarantine) must equal count(input)"


@pytest.mark.timeout(300)
def test_good_orders_reconcile(paimon_sql, submission, sth_oracle):
    _build_dq(paimon_sql, submission)
    rows = paimon_sql.batch(query="SELECT oid, cid, amt, ots FROM good_orders")
    sth_oracle(rows, _oracle_good())


@pytest.mark.timeout(300)
def test_quarantine_reasons_reconcile(paimon_sql, submission, sth_oracle):
    _build_dq(paimon_sql, submission)
    rows = paimon_sql.batch(query="SELECT oid, reason FROM quarantine")
    sth_oracle(rows, _oracle_quarantine())


@pytest.mark.timeout(300)
def test_mart_on_good_orders_reconciles(paimon_sql, submission, sth_oracle):
    _build_dq(paimon_sql, submission)
    # build the SCD2 dim and the as-of fct over ONLY the good orders.
    cdc_vals = ",".join(f"({c},{e},'{n}','{t}')" for c, e, n, t in CUST_CDC)
    paimon_sql.batch(ddl=[_CDC_DDL, _DIM_DDL, _FCT_DDL],
                     inserts=[f"INSERT INTO cust_cdc VALUES {cdc_vals}"])
    paimon_sql.batch(inserts=[
        "INSERT INTO dim_customer SELECT cid, tier, eff AS valid_from, "
        f"COALESCE(LEAD(eff) OVER (PARTITION BY cid ORDER BY eff), {SENTINEL}) "
        "AS valid_to FROM cust_cdc"])
    paimon_sql.batch(inserts=[
        "INSERT INTO fct_orders SELECT o.oid, o.cid, o.amt, d.tier "
        "FROM good_orders o JOIN dim_customer d "
        "ON o.cid=d.cid AND o.ots>=d.valid_from AND o.ots<d.valid_to"])
    rows = paimon_sql.batch(query="SELECT oid, cid, amt, tier FROM fct_orders")
    sth_oracle(rows, _oracle_fct())

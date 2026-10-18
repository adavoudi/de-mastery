"""MUTANT no-preagg — dashboard scans the raw fact instead of a rollup."""


def load_serving(con, rows):
    con.execute(
        "CREATE OR REPLACE TABLE fct_orders ("
        "order_id BIGINT, company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany(
        "INSERT INTO fct_orders VALUES (?, ?, ?, ?)",
        [(r["order_id"], r["company"], r["order_day"], r["revenue"]) for r in rows],
    )
    # BUG: no pre-aggregated serving table is built.


def dashboard_query(con, company):
    # BUG: scans the raw fact on every call (no rollup, no mart_daily).
    rows = con.execute(
        "SELECT order_day, SUM(revenue) FROM fct_orders WHERE company = ? "
        "GROUP BY order_day ORDER BY SUM(revenue) DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]


def olap_scan(con, company):
    rows = con.execute(
        "SELECT order_day, SUM(revenue) FROM fct_orders WHERE company = ? "
        "GROUP BY order_day ORDER BY SUM(revenue) DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]

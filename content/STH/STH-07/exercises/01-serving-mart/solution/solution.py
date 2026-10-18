"""STH-07 / 01-serving-mart — reference solution.

Serving layer over the merged streaming revenue mart. We load the merged rows
once, build a SMALL pre-aggregated rollup (`mart_daily`), and answer the
dashboard from the rollup pruned by company. `olap_scan` recomputes the same
answer straight from the raw merged fact for reconciliation.
"""


def load_serving(con, rows):
    con.execute(
        "CREATE OR REPLACE TABLE fct_orders ("
        "order_id BIGINT, company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany(
        "INSERT INTO fct_orders VALUES (?, ?, ?, ?)",
        [(r["order_id"], r["company"], r["order_day"], r["revenue"]) for r in rows],
    )
    # Pre-aggregated serving rollup: one row per (company, day).
    con.execute(
        "CREATE OR REPLACE TABLE mart_daily AS "
        "SELECT company, order_day, SUM(revenue) AS revenue "
        "FROM fct_orders GROUP BY company, order_day")


def dashboard_query(con, company):
    rows = con.execute(
        "SELECT order_day, revenue FROM mart_daily "
        "WHERE company = ? ORDER BY revenue DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]


def olap_scan(con, company):
    rows = con.execute(
        "SELECT order_day, SUM(revenue) AS revenue FROM fct_orders "
        "WHERE company = ? GROUP BY order_day ORDER BY revenue DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]

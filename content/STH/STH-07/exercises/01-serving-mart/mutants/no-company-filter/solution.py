"""MUTANT no-company-filter — dashboard ignores the company predicate."""


def load_serving(con, rows):
    con.execute(
        "CREATE OR REPLACE TABLE fct_orders ("
        "order_id BIGINT, company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany(
        "INSERT INTO fct_orders VALUES (?, ?, ?, ?)",
        [(r["order_id"], r["company"], r["order_day"], r["revenue"]) for r in rows],
    )
    con.execute(
        "CREATE OR REPLACE TABLE mart_daily AS "
        "SELECT company, order_day, SUM(revenue) AS revenue "
        "FROM fct_orders GROUP BY company, order_day")


def dashboard_query(con, company):
    # BUG: no WHERE company = ? — aggregates across ALL companies by day.
    rows = con.execute(
        "SELECT order_day, SUM(revenue) FROM mart_daily "
        "GROUP BY order_day ORDER BY SUM(revenue) DESC, order_day ASC",
    ).fetchall()
    return [(d, int(v)) for d, v in rows]


def olap_scan(con, company):
    rows = con.execute(
        "SELECT order_day, SUM(revenue) FROM fct_orders WHERE company = ? "
        "GROUP BY order_day ORDER BY SUM(revenue) DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]

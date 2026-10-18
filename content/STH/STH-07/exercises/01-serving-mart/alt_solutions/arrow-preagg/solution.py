"""STH-07 / 01-serving-mart — alt solution: Arrow-side pre-aggregation.

Genuinely different from the reference: the rollup is computed with PyArrow's
group-by compute kernels (not a DuckDB GROUP BY), then registered and persisted
as the `mart_daily` serving table. The dashboard still reads the small rollup
table and prunes by company.
"""

import pyarrow as pa


def load_serving(con, rows):
    con.execute(
        "CREATE OR REPLACE TABLE fct_orders ("
        "order_id BIGINT, company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany(
        "INSERT INTO fct_orders VALUES (?, ?, ?, ?)",
        [(r["order_id"], r["company"], r["order_day"], r["revenue"]) for r in rows],
    )
    # Roll up with Arrow compute kernels instead of SQL GROUP BY.
    t = pa.table({
        "company": pa.array([r["company"] for r in rows], pa.string()),
        "order_day": pa.array([r["order_day"] for r in rows], pa.string()),
        "revenue": pa.array([r["revenue"] for r in rows], pa.int64()),
    })
    agg = t.group_by(["company", "order_day"]).aggregate([("revenue", "sum")])
    agg = agg.rename_columns(["company", "order_day", "revenue"])
    con.register("mart_daily_arrow", agg)
    con.execute("CREATE OR REPLACE TABLE mart_daily AS "
                "SELECT company, order_day, CAST(revenue AS BIGINT) AS revenue "
                "FROM mart_daily_arrow")
    con.unregister("mart_daily_arrow")


def dashboard_query(con, company):
    rows = con.execute(
        "SELECT order_day, revenue FROM mart_daily WHERE company = ?",
        [company],
    ).fetchall()
    # sort in Python: revenue DESC, then order_day ASC.
    rows = sorted(((d, int(v)) for d, v in rows), key=lambda x: (-x[1], x[0]))
    return rows


def olap_scan(con, company):
    rows = con.execute(
        "SELECT order_day, SUM(revenue) FROM fct_orders WHERE company = ? "
        "GROUP BY order_day", [company],
    ).fetchall()
    return sorted(((d, int(v)) for d, v in rows), key=lambda x: (-x[1], x[0]))

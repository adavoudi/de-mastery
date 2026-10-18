"""STH-07 / 01-serving-mart — alt solution: pure-Python rollup, VALUES-materialised.

Genuinely different again: the rollup is computed with a plain Python dict
accumulator (no SQL aggregation, no Arrow), then materialised into the
`mart_daily` serving table via a parameterised INSERT. The dashboard reads the
rollup table and prunes by company.
"""

from collections import defaultdict


def load_serving(con, rows):
    con.execute(
        "CREATE OR REPLACE TABLE fct_orders ("
        "order_id BIGINT, company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    con.executemany(
        "INSERT INTO fct_orders VALUES (?, ?, ?, ?)",
        [(r["order_id"], r["company"], r["order_day"], r["revenue"]) for r in rows],
    )
    acc = defaultdict(int)
    for r in rows:
        acc[(r["company"], r["order_day"])] += int(r["revenue"])
    con.execute("CREATE OR REPLACE TABLE mart_daily "
                "(company VARCHAR, order_day VARCHAR, revenue BIGINT)")
    if acc:
        con.executemany(
            "INSERT INTO mart_daily VALUES (?, ?, ?)",
            [(c, d, v) for (c, d), v in acc.items()],
        )


def dashboard_query(con, company):
    rows = con.execute(
        "SELECT order_day, revenue FROM mart_daily "
        "WHERE company = ? ORDER BY revenue DESC, order_day ASC",
        [company],
    ).fetchall()
    return [(d, int(v)) for d, v in rows]


def olap_scan(con, company):
    acc = defaultdict(int)
    for d, v in con.execute(
            "SELECT order_day, revenue FROM fct_orders WHERE company = ?",
            [company]).fetchall():
        acc[d] += int(v)
    return sorted(acc.items(), key=lambda x: (-x[1], x[0]))

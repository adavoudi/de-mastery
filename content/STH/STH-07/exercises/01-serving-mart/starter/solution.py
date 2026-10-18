"""STH-07 / 01-serving-mart — starter. See README.md.

Build the serving rollup and answer the dashboard from it (not from the raw
fact). Replace the NotImplementedError bodies.
"""


def load_serving(con, rows):
    # TODO: load `rows` into a DuckDB table `fct_orders` (order_id, company,
    # order_day, revenue), then build a small pre-aggregated rollup table
    # `mart_daily` = SUM(revenue) GROUP BY company, order_day.
    raise NotImplementedError


def dashboard_query(con, company):
    # TODO: return [(order_day, revenue), ...] for `company`, ordered by revenue
    # DESC then order_day ASC, reading from `mart_daily` (NOT fct_orders).
    raise NotImplementedError


def olap_scan(con, company):
    # TODO: the same aggregate computed directly from the raw `fct_orders`.
    raise NotImplementedError

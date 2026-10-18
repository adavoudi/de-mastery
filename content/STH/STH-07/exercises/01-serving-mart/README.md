# STH-07 / 01 — Serve a sub-second dashboard off the streaming revenue mart

You run the **serving layer** on a streamhouse. Upstream, streams feed a Paimon primary-key fact
`fct_orders` (keyed by `order_id`, `merge-engine=partial-update`). Its *merged* state is one row per
order with the final `revenue`. A revenue dashboard must answer in about a second. It filters to one
company.

Your job: load the merged state into DuckDB **once**. Build a small **pre-aggregated** serving
rollup. Answer the dashboard query from the rollup (pruned by company), not from the raw fact.

## What you implement (`solution.py`)

```python
def load_serving(con, rows): ...        # build fct_orders + the mart_daily rollup in `con`
def dashboard_query(con, company): ...  # top days by revenue for one company, from mart_daily
def olap_scan(con, company): ...        # same aggregate computed directly from the raw merged rows
```

- `con` is an in-memory DuckDB connection (the serving engine).
- `rows` is the **merged** mart state. It is a list of dicts with keys
  `order_id` (int), `company` (str), `order_day` (str `YYYY-MM-DD`), `revenue` (int, minor units).
  The upstream Paimon merge has already deduplicated these rows to the latest value per `order_id`.
  You do **not** re-merge.

### `load_serving(con, rows)`

1. Load `rows` into a DuckDB table named **`fct_orders`** (columns above).
2. Build a small rollup table named **`mart_daily`** =
   `SELECT company, order_day, SUM(revenue) AS revenue FROM fct_orders GROUP BY company, order_day`.
   The dashboard reads this table.

### `dashboard_query(con, company)`

Return a list of `(order_day, revenue)` tuples for `company`. Order them by `revenue` DESC, then
`order_day` ASC. The query MUST read from `mart_daily` and apply the `company` predicate (so the
plan prunes). It must **not** scan `fct_orders`.

### `olap_scan(con, company)`

Compute the same result **directly from the raw merged rows** (`fct_orders`). The tests use it as
the OLAP-vs-rollup reconciliation. Return the same shape as `dashboard_query`.

## Constraints

- `revenue` is `SUM(revenue)`, in minor units (integer).
- Do not iterate a changelog and do not re-deduplicate. The input is already merged.
- `mart_daily` must be strictly smaller than `fct_orders` for realistic input (pre-aggregation).

## How the tests grade you (G1–G6)

- The dashboard result reconciles with an **independent DuckDB oracle** over the same merged rows
  (G6). It equals `olap_scan` for every company.
- `EXPLAIN dashboard_query` references `mart_daily` and never `fct_orders` (pruning / plan).
- Wall-clock time is only a **generous** guardrail. The tests do a warm-up. Then the median of
  several runs must beat a comfortable budget (this is robust on a loaded machine).
- The tests cross-check the upstream merged state against a batch "latest per key" rebuild (the
  streaming-vs-batch invariant).

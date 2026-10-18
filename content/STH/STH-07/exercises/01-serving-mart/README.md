# STH-07 / 01 — Serve a sub-second dashboard off the streaming revenue mart

You run the **serving layer** on a streamhouse. Upstream, a Paimon primary-key fact
`fct_orders` (keyed by `order_id`, `merge-engine=partial-update`) is fed by streams; its
*merged* state is one row per order with the final `revenue`. A revenue dashboard must answer in
about a second, filtered to one company.

Your job: load the merged state into DuckDB **once**, build a small **pre-aggregated** serving
rollup, and answer the dashboard query from the rollup (pruned by company) — not from the raw
fact.

## What you implement (`solution.py`)

```python
def load_serving(con, rows): ...        # build fct_orders + the mart_daily rollup in `con`
def dashboard_query(con, company): ...  # top days by revenue for one company, from mart_daily
def olap_scan(con, company): ...        # same aggregate computed directly from the raw merged rows
```

- `con` is an in-memory DuckDB connection (the serving engine).
- `rows` is the **merged** mart state: a list of dicts with keys
  `order_id` (int), `company` (str), `order_day` (str `YYYY-MM-DD`), `revenue` (int, minor units).
  These are already deduplicated to the latest value per `order_id` (the Paimon merge is applied
  upstream) — you do **not** re-merge.

### `load_serving(con, rows)`

1. Load `rows` into a DuckDB table named **`fct_orders`** (columns above).
2. Build a small rollup table named **`mart_daily`** =
   `SELECT company, order_day, SUM(revenue) AS revenue FROM fct_orders GROUP BY company, order_day`.
   This is the table the dashboard hits.

### `dashboard_query(con, company)`

Return a list of `(order_day, revenue)` tuples for `company`, **ordered by `revenue` DESC** then
`order_day` ASC. It MUST read from `mart_daily` and apply the `company` predicate (so the plan
prunes). It must **not** scan `fct_orders`.

### `olap_scan(con, company)`

The same result computed **directly from the raw merged rows** (`fct_orders`), used by the tests
as the OLAP-vs-rollup reconciliation. Return the same shape as `dashboard_query`.

## Constraints

- `revenue` is `SUM(revenue)`, in minor units (integer).
- Do not iterate a changelog or re-deduplicate — the input is already merged.
- `mart_daily` must be strictly smaller than `fct_orders` for realistic input (pre-aggregation).

## How you're graded (G1–G6)

- The dashboard result reconciles with an **independent DuckDB oracle** over the same merged rows
  (G6), and equals `olap_scan` for every company.
- `EXPLAIN dashboard_query` references `mart_daily` and never `fct_orders` (pruning / plan).
- Wall-clock is a **generous** guardrail only: warm-up, then the median of several runs must beat
  a comfortable budget (robust on a loaded machine).
- The upstream merged state itself is cross-checked against a batch "latest per key" rebuild
  (the streaming-vs-batch invariant).

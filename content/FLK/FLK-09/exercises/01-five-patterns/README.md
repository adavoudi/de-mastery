# Five streaming SQL patterns

Implement five Flink SQL queries, one for each streaming pattern. Each query is a function in `patterns.py`
that returns a **SQL string**. The test runs your SQL on an embedded Flink `TableEnvironment`
(the `flink_sql` fixture) over a fixed source. It grades the **changelog** (the RowKinds that the query emits)
and the **final materialized state** against an independent DuckDB answer. It never grades the SQL text, so
any query that computes the right answer passes.

## The fixed source

For `dedup_keep_first`, `top_n` and `split_merge`, the test creates this view before it runs your
query:

```sql
CREATE VIEW ord AS SELECT * FROM (VALUES
  ('A', 'o1', 10), ('A', 'o1', 10), ('A', 'o2', 50), ('A', 'o3', 30),
  ('B', 'o4', 20), ('B', 'o4', 20), ('B', 'o5', 90)
) AS t(company, order_id, amount);
```

Columns: `company STRING`, `order_id STRING`, `amount INT`. Note that `o1` and `o4` appear twice.

For `match_recognize` and `over_running_sum`, the test creates an **event-time** table `ev`
(backed by a CSV that the test writes). It has the columns `g STRING`, `v INT`, `ts TIMESTAMP(3)` and a
watermark on `ts`.

## What to implement in `patterns.py`

1. **`dedup_keep_first() -> str`** — From `ord`, keep exactly one row per `order_id`: the first one
   that the query sees (ties broken by `amount` ascending). Return the columns `order_id, amount`. (Append-only.)

2. **`top_n(n: int) -> str`** — From `ord`, return the top `n` orders by `amount` **per company**. Return
   the columns `company, amount`. (Updating.)

3. **`split_merge() -> str`** — Classify each row of `ord` as `'large'` when `amount >= 50`, else
   `'small'`. Write it as two filtered SELECTs that you merge together. Return the columns
   `order_id, bucket`, where `bucket` is `'large'`/`'small'`. (Append-only. Keep duplicates.)

4. **`match_recognize() -> str`** — Over `ev`, find every pair of consecutive rows (per `g`, in `ts`
   order) where the value strictly increases. Return the columns `g, v1, v2`. They are the earlier value and the
   later (larger) value.

5. **`over_running_sum() -> str`** — Over `ev`, emit `g, v, run_sum` for every row. `run_sum`
   is the running SUM of `v` per `g`, ordered by `ts` (unbounded preceding to current row).
   (Append-only, one row out for each row in.)

## Constraints

- Return the SQL as a string. Do not execute it yourself.
- Use the exact table/view and column names above.
- The test checks the changelog kinds (for example, dedup and OVER must be append-only, and Top-N must retract) and
  the final state against DuckDB. The column order does not matter, only the names that the task requests.

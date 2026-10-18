# Five streaming SQL patterns

Implement five Flink SQL queries, one per streaming pattern. Each is a function in `patterns.py`
that returns a **SQL string**. The test runs your SQL on an embedded Flink `TableEnvironment`
(the `flink_sql` fixture) over a fixed source and grades the **changelog** (the RowKinds it emits)
and the **final materialized state** against an independent DuckDB answer — never the SQL text, so
any query that computes the right answer passes.

## The fixed source

For `dedup_keep_first`, `top_n` and `split_merge`, the test creates this view before running your
query:

```sql
CREATE VIEW ord AS SELECT * FROM (VALUES
  ('A', 'o1', 10), ('A', 'o1', 10), ('A', 'o2', 50), ('A', 'o3', 30),
  ('B', 'o4', 20), ('B', 'o4', 20), ('B', 'o5', 90)
) AS t(company, order_id, amount);
```

Columns: `company STRING`, `order_id STRING`, `amount INT`. Note `o1` and `o4` are duplicated.

For `match_recognize` and `over_running_sum`, the test creates an **event-time** table `ev`
(backed by a CSV the test writes) with columns `g STRING`, `v INT`, `ts TIMESTAMP(3)` and a
watermark on `ts`.

## What to implement in `patterns.py`

1. **`dedup_keep_first() -> str`** — from `ord`, keep exactly one row per `order_id`: the first one
   seen (ties broken by `amount` ascending). Return columns `order_id, amount`. (Append-only.)

2. **`top_n(n: int) -> str`** — from `ord`, the top `n` orders by `amount` **per company**. Return
   columns `company, amount`. (Updating.)

3. **`split_merge() -> str`** — classify each row of `ord` as `'large'` when `amount >= 50` else
   `'small'`, by writing it as two filtered SELECTs merged back together. Return columns
   `order_id, bucket` where `bucket` is `'large'`/`'small'`. (Append-only; keep duplicates.)

4. **`match_recognize() -> str`** — over `ev`, find every pair of consecutive rows (per `g`, in `ts`
   order) where the value strictly increases. Return columns `g, v1, v2` = the earlier value and the
   later (larger) value.

5. **`over_running_sum() -> str`** — over `ev`, for every row emit `g, v, run_sum` where `run_sum`
   is the running SUM of `v` per `g` ordered by `ts` (unbounded preceding to current row).
   (Append-only, one row out per row in.)

## Constraints

- Return SQL as a string; do not execute it yourself.
- Use the exact table/view and column names above.
- The test checks the changelog kinds (e.g. dedup/OVER must be append-only; Top-N must retract) and
  the final state vs DuckDB. Column order does not matter beyond the names requested.

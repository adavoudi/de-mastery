# Same query on Spark, DuckDB (and optionally Trino)

One Iceberg table, `local.lkh08.sales`, partitioned by `region`, columns
`(id INT, region STRING, cat STRING, amt DOUBLE)`. You will run the **same logical query** on two
engines and prove they agree, then explain the plan.

The query (fixed):

```
SELECT cat, SUM(amt) AS total
FROM <sales>
WHERE region = 'r1' AND amt > 50
GROUP BY cat
```

## Part 1 — two engines, graded (no Docker)

In `solution.py` implement:

- `run_spark(spark, table)` → run the query against the Iceberg `table` and return the result
  (a Spark DataFrame with columns `cat`, `total`).
- `run_duckdb(con, parquet_glob)` → run the **same** query with DuckDB over `parquet_glob`
  (`read_parquet(parquet_glob, hive_partitioning=true)`), returning columns `cat`, `total`.

The test writes the same rows to the Iceberg table *and* to a Hive-partitioned Parquet copy, then
asserts your two results agree (independent engines ⇒ a trustworthy oracle).

## Part 2 — explain the plan, graded

Fill `plan_facts.yaml` with facts you read from Spark's actual physical plan
(`run_spark(...).explain("formatted")` or `df.explain(True)`). For this query on this
partitioned-by-`region` table:

- `data_filter_pushed`: is the `amt > 50` predicate pushed into the scan (predicate pushdown)?
- `partition_filter_pushed`: is the `region = 'r1'` predicate used to prune partitions (it appears
  in the scan's pushed filters)?

Set each to `true` or `false` to match what the plan **actually** shows. The test extracts the truth
from the real plan (it parses the Iceberg `BatchScan` `filters=...`) and fails you if your claim
disagrees — so look at the plan, don't guess.

## Part 3 — Trino, optional (needs the `core` lab)

If the `core` lab is running, a third test runs the same query on Trino and checks it agrees with
Spark. **This part skips cleanly when the lab is down** — it is a bonus layer, not required for the
grade. You may start it with `uv run de lab up core` (never run `de lab down`; other learners/agents
may be using it).

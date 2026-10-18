# Same query on Spark, DuckDB (and optionally Trino)

You have one Iceberg table, `local.lkh08.sales`. It is partitioned by `region`. Its columns are
`(id INT, region STRING, cat STRING, amt DOUBLE)`. You run the **same logical query** on two
engines and prove that they agree. Then you explain the plan.

The query (fixed):

```
SELECT cat, SUM(amt) AS total
FROM <sales>
WHERE region = 'r1' AND amt > 50
GROUP BY cat
```

## Part 1 — two engines, graded (no Docker)

In `solution.py`, implement:

- `run_spark(spark, table)` → run the query against the Iceberg `table` and return the result
  (a Spark DataFrame with the columns `cat`, `total`).
- `run_duckdb(con, parquet_glob)` → run the **same** query with DuckDB over `parquet_glob`
  (`read_parquet(parquet_glob, hive_partitioning=true)`). Return the columns `cat`, `total`.

The test writes the same rows to the Iceberg table *and* to a Hive-partitioned Parquet copy. Then
it asserts that your two results agree (independent engines ⇒ a trustworthy oracle).

## Part 2 — explain the plan, graded

Fill `plan_facts.yaml` with facts that you read from the actual physical plan of Spark
(`run_spark(...).explain("formatted")` or `df.explain(True)`). The table is partitioned by
`region`. For this query on this table, answer these questions:

- `data_filter_pushed`: is the `amt > 50` predicate pushed into the scan (predicate pushdown)?
- `partition_filter_pushed`: is the `region = 'r1'` predicate used to prune partitions (it appears
  in the pushed filters of the scan)?

Set each key to `true` or `false` to match what the plan **actually** shows. The test extracts the
truth from the real plan (it parses the `filters=...` of the Iceberg `BatchScan`). It fails you if
your claim disagrees. So look at the plan and do not guess.

## Part 3 — Trino, optional (needs the `core` lab)

Suppose the `core` lab is running. Then a third test runs the same query on Trino and checks that
it agrees with Spark. **This part skips cleanly when the lab is down.** It is a bonus layer. The
grade does not require it. You may start the lab with `uv run de lab up core`. Never run
`de lab down`, because other learners or agents may use the lab.

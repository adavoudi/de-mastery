# SPK-05 · Catalyst and the physical plan

## Why Catalyst exists

When you write a dbt model you describe *what* you want, and Redshift's planner decides *how* to
get it: which join order, whether to use a hash or merge join, which predicates to push down to the
scan. You trust `EXPLAIN` to show you that plan and you read it to find the slow part. Spark's
**Catalyst** optimizer plays exactly that role for DataFrame and SQL queries. You hand it a
declarative description; it rewrites it through a pipeline of plans and hands the physical plan to
the execution engine.

The reason this matters: in Spark the single biggest lever on a job's cost is not CPU, it is **how
much data gets read and shuffled**. Catalyst's job is to minimise both — by reading fewer columns
(column pruning), fewer rows (predicate pushdown), and doing fewer/cheaper shuffles. If you can
read a physical plan, you can tell at a glance whether Catalyst got those wins or whether something
in your code blocked them. That skill is the whole module.

## How it works

Catalyst runs a query through four stages, and `df.explain("extended")` shows all of them:

1. **Parsed / unresolved logical plan** — the query as written, column and table references not yet
   bound.
2. **Analyzed logical plan** — references resolved against the catalog; types checked.
3. **Optimized logical plan** — rule-based rewrites: constant folding, predicate pushdown toward
   the source, column pruning, filter reordering, `null` propagation, boolean simplification. This
   is where `WHERE` clauses migrate down next to the scan.
4. **Physical plan** — one or more candidate physical plans; a cost model picks one. This is what
   actually runs. `df.explain("formatted")` prints it as a numbered tree with a details section.

![Catalyst rewrites the logical plan, then picks one physical plan to run](diagrams/catalyst-pipeline.svg)

Two properties you will read off the physical plan over and over:

- **`PushedFilters`** — printed on the `FileScan`/`BatchScan` node. A non-empty list means Spark
  pushed those predicates *into* the Parquet reader, so whole row groups are skipped using
  footer statistics and never deserialized. An empty list on a filter you expected to push is a
  red flag (often caused by wrapping the column in a non-pushable function, or a Python UDF).
- **`ReadSchema`** — the set of columns the scan actually reads. If you only need two columns, a
  narrow `ReadSchema` proves column pruning happened. Parquet is columnar (FND-01), so a narrow
  read is a real I/O win, not a cosmetic one.

Spark exposes the modes through `explain(mode)` where mode is `"simple"`, `"extended"`,
`"codegen"`, `"cost"`, or `"formatted"`. `"formatted"` is the one you read for structure;
`"cost"` adds the optimizer's row/size estimates.

## From your world

| Redshift / dbt | Spark / Catalyst | Note |
|---|---|---|
| `EXPLAIN` output | `df.explain("formatted")` | read the plan, don't guess |
| Predicate pushed to the scan / zone maps skip blocks | `PushedFilters` on `FileScan` + Parquet row-group skipping | same idea, Parquet footer stats |
| Column store reads only referenced columns | narrow `ReadSchema` (column pruning) | columnar win |
| Planner reorders joins / pushes `WHERE` early | optimized logical plan rewrites | rule-based, before physical |
| `DISTSTYLE`/`SORTKEY` influence the plan | AQE + partitioning (SPK-07/08) | later modules |

The mental model is identical to reading a Redshift `EXPLAIN`: find where the rows are, see what
got filtered before the expensive step, confirm the scan is narrow.

## When NOT to lean on it

- **Don't block pushdown with a Python UDF.** A `BatchEvalPython` or opaque UDF in a filter stops
  Catalyst from pushing that predicate; keep filters in native column expressions (SPK-09).
- **Don't assume a `.cache()` or an intermediate `collect()` makes it faster.** Materialising a
  wide intermediate can read *more* data than letting Catalyst prune and push through the whole
  pipeline. Read the plan before and after.
- **Don't micro-tune the plan by hand** when the real fix is upstream (fewer columns selected, a
  filter written against the partition column, a broadcast hint — SPK-06). Catalyst is good; your
  job is to not get in its way.
- **`explain` is not a benchmark.** It tells you the shape of the work, not the wall-clock. Use it
  to reason about I/O and shuffles, not to claim one plan is "X ms faster".

## Further reading

Pinned to Spark **4.0.0**:

- SQL performance tuning (pushdown, pruning, hints):
  <https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html>
- `DataFrame.explain`:
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html>
- SQL programming guide (Catalyst, data sources):
  <https://spark.apache.org/docs/4.0.0/sql-programming-guide.html>
- Parquet data source (filter pushdown options):
  <https://spark.apache.org/docs/4.0.0/sql-data-sources-parquet.html>

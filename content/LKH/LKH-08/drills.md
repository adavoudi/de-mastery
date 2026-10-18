# LKH-08 · Interview drills

**1. "A lakehouse query is slow. How do you tell if the engine is pruning and pushing down?"**
> Read the plan. In Spark's `explain(formatted)` the Iceberg `BatchScan` shows `filters=...`; I check
> that both the partition-column predicate (pruning) and the data predicate (pushdown) are listed
> there. For plain file sources I look for `PartitionFilters` and `PushedFilters`. If the predicate
> only shows up as a `Filter` node above a full scan, the engine is reading everything and filtering
> late — that's the bug.

**2. "Partition pruning vs predicate pushdown — one sentence each."**
> Partition pruning eliminates whole partitions first, using a predicate on the partition column.
> Predicate pushdown pushes the predicate into the scan so per-file/row-group min-max statistics skip
> chunks within the partitions still read. Coarse-then-fine; both cut bytes scanned.

**3. "Spark and DuckDB disagree on a GROUP BY total over the same table. Who's right?"**
> Neither format nor engine should cause that: on the same committed snapshot two correct engines
> must agree. The discrepancy is a query-logic difference — a mismatched predicate, NULL handling, or
> a type coercion in one of them. I'd diff the two queries, not suspect the table. (This is exactly
> why a second engine makes a good oracle.)

**4. "Same table — when Trino/Athena, when Spark, when DuckDB?"**
> Trino/Athena for interactive, latency-sensitive BI (Athena serverless, billed by bytes scanned, so
> pruning matters). Spark for heavy batch ETL, streaming and ML pipelines. DuckDB for single-node,
> in-process analytics on data that fits locally — great for ad-hoc and as a correctness oracle. Pick
> by workload, same data underneath.

**5. "What do Iceberg Puffin files have to do with query speed?"**
> They store table statistics (e.g. NDV sketches). A cost-based optimizer reads them to choose join
> order and strategy; without stats it guesses and can pick a bad plan. It's the lake equivalent of
> running `ANALYZE` on Redshift so the planner has accurate cardinalities.

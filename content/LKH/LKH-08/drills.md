# LKH-08 · Interview drills

**1. "A lakehouse query is slow. How do you tell if the engine is pruning and pushing down?"**
> Read the plan. In `explain(formatted)` of Spark, the Iceberg `BatchScan` shows `filters=...`. I
> check that this list has both the partition-column predicate (pruning) and the data predicate
> (pushdown). For plain file sources, I look for `PartitionFilters` and `PushedFilters`. Suppose
> the predicate only shows up as a `Filter` node above a full scan. Then the engine reads
> everything and filters late. That is the bug.

**2. "Partition pruning vs predicate pushdown — one sentence each."**
> Partition pruning first eliminates whole partitions, with a predicate on the partition column.
> Predicate pushdown pushes the predicate into the scan. Then per-file and per-row-group min-max
> statistics skip chunks inside the partitions that the engine still reads. The order is coarse
> first, then fine. Both cut bytes scanned.

**3. "Spark and DuckDB disagree on a GROUP BY total over the same table. Who's right?"**
> Neither the format nor the engine should cause this. On the same committed snapshot, two correct
> engines must agree. The difference is in the query logic. For example, a predicate does not
> match, NULL handling differs, or one engine does a type coercion. I would compare the two
> queries. I would not suspect the table. (This is exactly why a second engine makes a good
> oracle.)

**4. "Same table — when Trino/Athena, when Spark, when DuckDB?"**
> Use Trino/Athena for interactive, latency-sensitive BI. Athena is serverless and bills by bytes
> scanned, so pruning matters. Use Spark for heavy batch ETL, streaming and ML pipelines. Use
> DuckDB for single-node, in-process analytics on data that fits locally. It is great for ad-hoc
> work and as a correctness oracle. Pick by workload. The data underneath is the same.

**5. "What do Iceberg Puffin files have to do with query speed?"**
> They store table statistics (for example NDV sketches). A cost-based optimizer reads them to
> choose the join order and strategy. Without stats, it guesses and can pick a bad plan. This is
> the lake equivalent of `ANALYZE` on Redshift. Then the planner has accurate cardinalities.

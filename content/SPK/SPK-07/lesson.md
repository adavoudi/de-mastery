# SPK-07 · Partitioning, bucketing, and the small-file problem

## Why file layout is a first-class concern

You already know from Athena and Iceberg that *how data is laid out on disk* decides query cost.
Follow these three rules, and you win before the engine starts:

- Partition on a column that you always filter by.
- Keep files big enough to spread the cost of opening a file and reading its metadata.
- Keep files small enough to read in parallel.

Spark has two different "partition" concepts. A common mistake for beginners is to treat them as one concept.

- **In-memory partitions** — how Spark splits a DataFrame across tasks *while it computes*. You control
  them with `repartition`, `coalesce`, and `spark.sql.shuffle.partitions`.
- **On-disk partitions** — the directory layout of the written output. You control them with
  `write.partitionBy("col")`. It creates `col=value/` folders (Hive-style), exactly like your
  Athena/Iceberg partitions.

The number of **output files** per write is roughly equal to the number of in-memory partitions at
write time, multiplied across the on-disk partition folders. So the two concepts meet exactly at
write time. At that point, the small-file problem starts or is avoided.

## repartition vs coalesce

- **`repartition(n)`** does a full shuffle to produce exactly `n` evenly-sized partitions (wide,
  adds an `Exchange`). Use it to *increase* parallelism or to rebalance skew.
- **`coalesce(n)`** merges existing partitions *without a shuffle* (narrow, no `Exchange`). It can only
  *reduce* the count. Use it to cut the file count cheaply after a filter has made the data smaller.
  It can create skew, because it only joins existing partitions together.
- `repartition(n, col)` and `repartitionByRange` let you control the partitioning *expression*. This
  matters for bucketing-like effects and for writing one file per key group.

The key contrast: *to reduce files cheaply after a selective filter, use `coalesce`. To
rebalance or grow partitions, use `repartition`.*

![coalesce merges neighbouring partitions with no shuffle and may stay uneven; repartition shuffles every partition into even ones](diagrams/repartition-vs-coalesce.svg)

## The small-file problem

Thousands of tiny files make a query slow. Each file costs a metadata lookup and an open. Listing
the files takes most of the time. Readers also cannot align to row-group boundaries. In your Athena
world, the symptom is a query that spends most of its time in S3 listing.

To fix this, follow these steps:

1. **Target a file size** (commonly ~128 MB–1 GB).
2. Calculate the partition count with `total_bytes / target_size`.
3. Use `repartition` or `coalesce` to get that count before you write.

In the tiny lab, you target a **file count** directly. The reasoning is the same.

There is a trap. If you use `coalesce(1)` to "get one file", *all* data goes through one task. This is
fine for a 10 MB result. It is catastrophic for 100 GB. The right habit is to calculate the count from
the data size. Do not hard-code 1.

## Bucketing

**Bucketing** pre-shuffles a table on a key into a fixed number of buckets *at write time*
(`write.bucketBy(n, "key").sortBy("key").saveAsTable(...)`). You can then join two bucketed tables on
the bucket key **without a shuffle at read time**. This works because matching keys are already
colocated (stored together). It is the Spark analogue of choosing a Redshift `DISTKEY` so that joins are
local. You pay the cost once, at write time. Every later join on that key avoids an `Exchange`.
Bucketing needs a managed table (`saveAsTable`), not a bare path.

## From your world

| Redshift / Athena / Iceberg | Spark | Note |
|---|---|---|
| `DISTKEY` → local joins | `bucketBy(n, key)` | pre-shuffle once, join shuffle-free |
| `SORTKEY` | `sortBy` within buckets / `sortWithinPartitions` | clustering for scans |
| Athena/Iceberg partition columns | `write.partitionBy(col)` | `col=value/` dirs |
| S3 listing dominated by tiny files | target file size → derive partition count | small-file fix |
| Repartition a table to rebalance | `repartition(n)` (shuffle) | wide |
| Merge files after a filter | `coalesce(n)` (no shuffle) | narrow |

## When NOT to do these

- **Don't use `coalesce(1)` on large data** to make one file. It sends everything through one task.
  Calculate the count from the size.
- **Don't use `partitionBy` on a high-cardinality column** (for example `customer_id`). You get millions
  of tiny directories. This is the worst small-file explosion. Partition on low-cardinality columns
  that you filter on often (date, company).
- **Don't bucket unless the join key is stable and reused.** Bucketing has a write-time cost and
  fixes the bucket count. It only helps if you join on that key many times.
- **Don't confuse `repartition` with `partitionBy`.** One is the in-memory task layout. The other is
  the on-disk directory layout. They interact at write time, but they are not the same thing.

## Further reading

Pinned to Spark **4.0.0**:

- SQL performance tuning (coalesce/repartition, AQE partition coalescing):
  <https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html>
- Bucketing, sorting and partitioning (persistent tables):
  <https://spark.apache.org/docs/4.0.0/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning>
- `DataFrame.repartition` / `DataFrame.coalesce`:
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html>
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html>

# SPK-07 / 03 · Partition the output by a low-cardinality key

You make the same decision for Athena/Iceberg when you choose the partition column. Partition on a
**low-cardinality column that you filter on often**. Then each partition is a unit of reasonable size
that the engine can prune. If you partition on a high-cardinality column (one directory per order), you
get the classic small-file explosion.

## Task

Implement `write_partitioned(df, out_dir)`. It writes `df` as Parquet into `out_dir`, physically
partitioned by **`company_id`** (Hive-style `company_id=<value>/` directories), with the data unchanged.
Return the output path as a string.

## Constraints

- Partition by `company_id` (low cardinality: a few companies), **not** by a high-cardinality
  column such as `order_id`.
- Overwrite any existing output.

## How it is graded

- **Layout:** `out_dir` contains exactly one `company_id=<value>/` directory for each distinct
  `company_id` in the input, and no more. This proves that you did not partition by a high-cardinality key.
- **Data (DuckDB oracle):** when you read the whole output back, it equals the input rows. When you read
  a single partition directory, it equals the rows for that company (partition pruning works).

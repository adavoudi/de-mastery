# SPK-07 / 03 · Partition the output by a low-cardinality key

Choosing the partition column is the same decision you make for Athena/Iceberg: partition on a
**low-cardinality, frequently-filtered** column so each partition is a reasonably sized, prunable
unit. Partitioning on a high-cardinality column (one directory per order) is the classic small-file
explosion.

## Task

Implement `write_partitioned(df, out_dir)` that writes `df` as Parquet into `out_dir`, physically
partitioned by **`company_id`** (Hive-style `company_id=<value>/` directories), data unchanged.
Return the output path as a string.

## Constraints

- Partition by `company_id` (low cardinality: a few companies), **not** by a high-cardinality
  column such as `order_id`.
- Overwrite any existing output.

## How it is graded

- **Layout:** `out_dir` contains exactly one `company_id=<value>/` directory per distinct
  `company_id` in the input (and no more — proof you did not partition by a high-cardinality key).
- **Data (DuckDB oracle):** reading the whole output back equals the input rows. Reading a single
  partition directory equals the rows for that company (partition pruning works).

# Write partitioned output with the right save mode

A nightly job writes the orders table to a lake path, partitioned by company so downstream reads
can prune. The job must be **idempotent**: running it twice must leave exactly one copy of the
data, not two.

Implement `write_partitioned(df, out_path)` in `solution.py`. It must write `df` to `out_path` as
Parquet such that:

1. The output is **partitioned by `company_id`** (one `company_id=.../` directory per company).
2. The save mode is **`overwrite`**, so re-running the job replaces the previous output instead of
   appending a second copy.

The function writes data; it does not need to return anything.

The test will:
- call `write_partitioned` once, read the path back, and compare to the input (an independent
  check that no rows were lost or duplicated),
- confirm the `company_id=...` partition directories exist,
- call `write_partitioned` **again** and confirm the row count is unchanged (this is where
  `append` would fail — it would double the data).

Partition column choice matters: `company_id` is low-cardinality and queries filter on it, so it
prunes well. Partitioning by a high-cardinality key like `order_id` would create a flood of tiny
files — don't.

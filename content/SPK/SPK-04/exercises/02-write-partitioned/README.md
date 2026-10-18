# Write partitioned output with the right save mode

A nightly job writes the orders table to a lake path. The data is partitioned by company, so
downstream reads can prune. The job must be **idempotent**. If you run it twice, exactly one
copy of the data must remain, not two.

Implement `write_partitioned(df, out_path)` in `solution.py`. It must write `df` to `out_path`
as Parquet. The output must meet these conditions:

1. The output is **partitioned by `company_id`** (one `company_id=.../` directory for each
   company).
2. The save mode is **`overwrite`**. Then, when you run the job again, it replaces the previous
   output. It does not append a second copy.

The function writes data. It does not need to return anything.

The test does these steps:
- It calls `write_partitioned` once, reads the path back, and compares the result to the input.
  This is an independent check that no rows are lost or duplicated.
- It confirms that the `company_id=...` partition directories exist.
- It calls `write_partitioned` **again** and confirms that the row count is the same. This is
  where `append` would fail, because it would double the data.

The partition column matters. `company_id` has low cardinality and queries filter on it, so it
prunes well. Do not partition by a high-cardinality key like `order_id`. It would create a flood
of tiny files.

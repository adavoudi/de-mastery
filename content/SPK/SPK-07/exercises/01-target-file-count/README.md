# SPK-07 / 01 · Hit a target output file count

The number of files that you write is the main lever for the small-file problem. If you write too many
files, listing takes most of the time. If you write too few, you lose parallelism. In production, you
calculate the count from `total_bytes / target_size`. At lab scale, you target the **count** directly.
This uses the same mechanism: the number of in-memory partitions at write time controls the number of
part files.

## Task

Implement `write_balanced(df, out_dir, num_files)`. It writes `df` as Parquet into `out_dir` so
that it produces **exactly `num_files` part files**, with the data unchanged. Return the output
path as a string.

## Constraints

- Honor `num_files`. Do **not** hard-code `coalesce(1)`.
- Produce balanced partitions (a full shuffle to `num_files` partitions is expected).
- Overwrite any existing output so the write is deterministic.

## How it is graded

- **File count:** the number of `part-*.parquet` files in `out_dir` equals `num_files` (tested for
  `num_files` = 2 and 4).
- **Data (DuckDB oracle):** when you read the written files back, you get exactly the input rows
  (order-independent).

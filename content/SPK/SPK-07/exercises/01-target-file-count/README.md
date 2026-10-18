# SPK-07 / 01 · Hit a target output file count

Writing data as the right number of files is the small-file lever. Too many files and listing
dominates; too few and you lose parallelism. In production you derive the count from
`total_bytes / target_size`; at lab scale you target the **count** directly, which exercises the
same mechanism: the number of in-memory partitions at write time controls the number of part files.

## Task

Implement `write_balanced(df, out_dir, num_files)` that writes `df` as Parquet into `out_dir` so
that **exactly `num_files` part files** are produced, with the data unchanged. Return the output
path as a string.

## Constraints

- Honor `num_files` — do **not** hard-code `coalesce(1)`.
- Produce balanced partitions (a full shuffle to `num_files` partitions is expected).
- Overwrite any existing output so the write is deterministic.

## How it is graded

- **File count:** the number of `part-*.parquet` files in `out_dir` equals `num_files` (tested for
  `num_files` = 2 and 4).
- **Data (DuckDB oracle):** reading the written files back yields exactly the input rows
  (order-independent).

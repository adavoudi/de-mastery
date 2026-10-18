# SPK-04 · Interview drills

**1. "temp view vs. a persisted table — when do you use each?"**
> A temp view names a lazy plan for the current session and writes nothing; use it for
> intermediate steps you reference by name (like a dbt `ref`). A persisted table (`saveAsTable`)
> materializes bytes and survives the session; use it when downstream jobs or tools must read the
> result. For a managed table, DROP deletes the files; for external, it doesn't.

**2. "Default save mode bites a nightly job. What happened?"**
> They used `append` where they meant `overwrite`. The refresh added a second full copy on top of
> the previous run, so every count and sum doubled. `overwrite` replaces; `append` adds.

**3. "Why not inferSchema on a production CSV?"**
> It needs an extra scan and guesses types — e.g. reading a zero-padded id as an int and losing
> the padding, or typing a sparse column wrong. Supply an explicit `StructType` (and a `nullValue`
> for empty strings) so reads are deterministic.

**4. "What does partitionBy buy you, and how can it hurt?"**
> It writes one directory per value, so filters on that column prune directories at read time
> (partition pruning). It hurts when the column is high-cardinality (e.g. an id): you get a
> directory per value and a flood of tiny files, which is slow to list and read.

**5. "Managed vs. external table — one-line difference?"**
> Managed: Spark owns metadata and data, so DROP deletes the files. External (explicit
> LOCATION/path): Spark owns only metadata, so DROP leaves the files. Same idea as Iceberg/Glue
> managed vs. external.

**6. "Is Spark SQL slower than the DataFrame API?"**
> No. Both front ends compile to the same Catalyst logical plan and are optimized identically.
> Pick whichever is clearer for the task.

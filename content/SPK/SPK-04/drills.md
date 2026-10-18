# SPK-04 · Interview drills

**1. "temp view vs. a persisted table — when do you use each?"**
> A temp view names a lazy plan for the current session and writes nothing. Use it for
> intermediate steps that you refer to by name (like a dbt `ref`). A persisted table
> (`saveAsTable`) materializes bytes and survives the session. Use it when downstream jobs or
> tools must read the result. For a managed table, DROP deletes the files. For an external
> table, it does not.

**2. "Default save mode bites a nightly job. What happened?"**
> They used `append` where they meant `overwrite`. The refresh added a second full copy on top of
> the previous run. So every count and sum doubled. `overwrite` replaces. `append` adds.

**3. "Why not inferSchema on a production CSV?"**
> It needs an extra scan and it guesses types. For example, it may read a zero-padded id as an
> int and lose the padding, or it may type a sparse column wrong. Supply an explicit
> `StructType` (and a `nullValue` for empty strings). Then reads are deterministic.

**4. "What does partitionBy buy you, and how can it hurt?"**
> It writes one directory for each value. So filters on that column prune directories at read
> time (partition pruning). It hurts when the column has high cardinality (for example, an id).
> You get one directory for each value and a flood of tiny files. These files are slow to list
> and read.

**5. "Managed vs. external table — one-line difference?"**
> Managed: Spark owns the metadata and the data, so DROP deletes the files. External (explicit
> LOCATION/path): Spark owns only the metadata, so DROP leaves the files. This is the same idea
> as managed vs. external in Iceberg/Glue.

**6. "Is Spark SQL slower than the DataFrame API?"**
> No. Both front ends compile to the same Catalyst logical plan, and Spark optimizes them in the
> same way. Pick the one that is clearer for the task.

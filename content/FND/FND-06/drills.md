# Interview drills — FND-06

### 1. "Why columnar?"
**Model answer.** Analytical queries read a few columns across many rows. A columnar format stores
each column contiguously. So the engine reads only the bytes of the columns that the query
references. It does not read every field of every row. This is often a 10–100× reduction.

Contiguity also makes compression and encoding (dictionary, run-length, delta) much more
effective, because adjacent values share a type and a pattern. Also, the min/max stats of each row
group let the engine skip whole row groups that a filter cannot match. Row formats (CSV, Avro)
cannot do any of this for scans of wide columns.

### 2. "Walk me through how Parquet skips data for a WHERE clause."
**Model answer.** A Parquet file is a set of row groups. Each row group has a column chunk per
column. Each chunk is split into pages. The footer stores statistics for each chunk, including min
and max.

Take `WHERE x > 900`. The engine reads the footer. For each row group, it checks `max(x)`. If
`max(x) <= 900`, no row can match. So it prunes the entire row group without reading its pages.

Range and equality predicates prune whenever the `[min, max]` of the group does not overlap the
predicate. The win depends on the sort or cluster order. Tight, disjoint ranges prune a lot.
Random order prunes nothing.

### 3. "A query on Parquet still scans the whole file. Why?"
**Model answer.** Most likely the filtered column is not sorted or clustered. Then the `[min,
max]` of every row group spans the whole domain, and no group can be pruned.

Other causes:

- The writer did not write stats for that column (or it is a complex type without stats).
- The row groups are huge, so even one match forces a big read.
- The predicate is not pushable. For example, a function around the column defeats the min/max
  comparison.

Fixes:

- Sort or cluster on the filter column (or set an Iceberg sort order).
- Choose a good size for row groups.
- Keep the predicate directly on the raw column.

### 4. "How does this reduce an Athena bill?"
**Model answer.** Athena bills by bytes scanned. The first lever is to store Parquet and select
only the columns that you need. Column pruning alone can cut cost by an order of magnitude,
compared to `SELECT *` on CSV/JSON.

The second lever is to partition the data and set a sort order. Then Athena prunes partitions and
row groups for selective `WHERE` clauses. On Iceberg, the same stats live in manifests. So it
prunes whole files at planning time. Together, projection and pruning are the main cost controls.

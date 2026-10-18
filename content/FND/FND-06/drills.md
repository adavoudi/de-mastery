# Interview drills — FND-06

### 1. "Why columnar?"
**Model answer.** Analytical queries read a few columns across many rows. A columnar format stores
each column contiguously, so the engine reads only the referenced columns' bytes instead of every
field of every row — often a 10–100× reduction. Contiguity also makes compression and encoding
(dictionary, run-length, delta) much more effective because adjacent values share a type and
pattern. And per-row-group min/max stats let the engine skip whole row groups a filter can't match.
Row formats (CSV, Avro) can't do any of that for wide scans.

### 2. "Walk me through how Parquet skips data for a WHERE clause."
**Model answer.** A Parquet file is row groups, each with a column chunk per column, each chunk
split into pages, and the footer stores per-chunk statistics including min and max. For
`WHERE x > 900`, the engine reads the footer and, for each row group, checks `max(x)`: if
`max(x) <= 900`, no row can match, so it prunes the entire row group without reading its pages.
Range and equality predicates prune whenever the group's `[min, max]` doesn't overlap the
predicate. The win depends on sort/cluster order — tight, disjoint ranges prune a lot; random
order prunes nothing.

### 3. "A query on Parquet still scans the whole file. Why?"
**Model answer.** Most likely the filtered column isn't sorted or clustered, so every row group's
`[min, max]` spans the whole domain and no group can be pruned. Other causes: stats weren't written
for that column (or it's a complex type without stats), the row groups are huge so even one match
forces a big read, or the predicate isn't pushable (e.g. a function wrapping the column defeats
min/max comparison). Fixes: sort/cluster on the filter column (or set an Iceberg sort order),
right-size row groups, and keep the predicate directly on the raw column.

### 4. "How does this reduce an Athena bill?"
**Model answer.** Athena bills by bytes scanned. First lever: store Parquet and select only the
columns you need — column pruning alone can cut cost by an order of magnitude versus `SELECT *` on
CSV/JSON. Second lever: partition the data and set a sort order so Athena prunes partitions and row
groups for selective `WHERE`s. On Iceberg the same stats live in manifests, so it prunes whole files
at planning time. Together, projection + pruning are the dominant cost controls.

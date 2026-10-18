# Interview drills — STH-07

### 1. Why does a dashboard on a streamhouse read a rollup instead of the raw fact?
**Model answer.** The raw streaming fact is laid out for *ingestion* — many small files, frequent
commits, LSM levels, and (for MOR) delete files — which is expensive to scan interactively. A
dashboard tile has a ~1 s budget, so you precompute a small aggregate (revenue per company per
day) and the dashboard reads that. Rows read drop from millions to dozens, and a columnar engine
prunes it further with the dashboard's predicate. It's the same discipline as a BI rollup /
materialized view in a batch warehouse; the streamhouse just makes the rollup fresher.

### 2. How do you read the *current* state of a Paimon primary-key table for serving?
**Model answer.** You do a *batch snapshot* read, which applies the merge engine (dedup /
partial-update / aggregation) and returns the latest value per key — via `pypaimon`
(`CatalogFactory` → `new_read_builder`) or a Flink batch `SELECT`. You do **not** iterate the
changelog: that's an unbounded stream meant for downstream pipelines, not a point-in-time
dashboard. Reading the changelog and summing it would double-count superseded versions.

### 3. How do you *prove* a serving query is fast for the right reason, not by luck?
**Model answer.** Timing alone is flaky (cache state, a loaded box, GC). The durable proof is the
**plan**: `EXPLAIN` the dashboard query and assert it reads the pre-aggregated rollup table and
applies the predicate (pruning), not a full scan of the raw fact. Wall-clock is a *generous*
guardrail only — warm the cache, take the median of several runs, and allow slack for a busy
machine.

### 4. Your streaming dashboard shows €2.1 M but the nightly batch rebuild shows €1.4 M. Where do you look first?
**Model answer.** Most likely you're reading the changelog or un-merged versions instead of the
merged snapshot, so superseded rows are double-counted — classic when a partial-update or
aggregation table is read without the merge applied. Other suspects: a stale snapshot (serving an
old checkpoint), a missing company/date predicate pulling in other partitions, or MOR delete
files not applied (pyiceberg 0.9.1 does not apply v2 deletes — read merged state via a Flink
batch job or collapse in DuckDB). Reconcile the OLAP answer against a batch recompute of the same
events (G6) to localise it.

### 5. DuckDB's `iceberg_scan` fails to open your Flink-written Iceberg table. What do you do?
**Model answer.** The Flink hadoop catalog writes manifest paths with a `file:` scheme that
DuckDB's iceberg extension rejects. Read the table with **pyiceberg** (`StaticTable.from_metadata`
→ `scan().to_arrow()`), then `register` the Arrow table in DuckDB and run SQL over it. Same answer,
avoids the scheme mismatch. For COW / append-only tables this is clean; for MOR/upsert, read the
merged state via a Flink batch job because pyiceberg 0.9.1 won't apply v2 deletes.

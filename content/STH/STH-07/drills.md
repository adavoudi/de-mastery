# Interview drills — STH-07

### 1. Why does a dashboard on a streamhouse read a rollup instead of the raw fact?
**Model answer.** The layout of the raw streaming fact is for *ingestion*. It has many small files,
frequent commits, LSM levels and (for MOR) delete files. This layout is expensive to scan
interactively. A dashboard tile has a budget of ~1 s. So you precompute a small aggregate (revenue
per company per day), and the dashboard reads that. The rows that it reads drop from millions to
dozens. A columnar engine prunes them further with the predicate of the dashboard. This is the same
discipline as a BI rollup / materialized view in a batch warehouse. The streamhouse only makes the
rollup fresher.

### 2. How do you read the *current* state of a Paimon primary-key table for serving?
**Model answer.** You do a *batch snapshot* read. It applies the merge engine (dedup /
partial-update / aggregation) and returns the latest value per key. You can use `pypaimon`
(`CatalogFactory` → `new_read_builder`) or a Flink batch `SELECT`. You do **not** iterate the
changelog. The changelog is an unbounded stream for downstream pipelines. It is not for a
point-in-time dashboard. If you read the changelog and sum it, you double-count superseded versions.

### 3. How do you *prove* a serving query is fast for the right reason, not by luck?
**Model answer.** Timing alone is flaky (cache state, a loaded machine, GC). The durable proof is
the **plan**. Run `EXPLAIN` on the dashboard query. Assert that it reads the pre-aggregated rollup
table and applies the predicate (pruning). Assert that it does not do a full scan of the raw fact.
Wall-clock time is only a *generous* guardrail. Warm the cache, take the median of several runs,
and allow slack for a busy machine.

### 4. Your streaming dashboard shows €2.1 M but the nightly batch rebuild shows €1.4 M. Where do you look first?
**Model answer.** Most likely you read the changelog or un-merged versions and not the merged
snapshot. Then you double-count superseded rows. This is a classic problem when you read a
partial-update or aggregation table without the merge applied. Other suspects:

- A stale snapshot (you serve an old checkpoint).
- A missing company/date predicate that pulls in other partitions.
- MOR delete files that are not applied. pyiceberg 0.9.1 does not apply v2 deletes. Read the merged
  state via a Flink batch job, or collapse it in DuckDB.

Reconcile the OLAP answer against a batch recompute of the same events (G6) to find the cause.

### 5. DuckDB's `iceberg_scan` fails to open your Flink-written Iceberg table. What do you do?
**Model answer.** The Flink hadoop catalog writes manifest paths with a `file:` scheme. The iceberg
extension of DuckDB rejects this scheme. Read the table with **pyiceberg**
(`StaticTable.from_metadata` → `scan().to_arrow()`). Then `register` the Arrow table in DuckDB and
run SQL over it. You get the same answer and avoid the scheme mismatch. This works cleanly for COW /
append-only tables. For MOR/upsert tables, read the merged state via a Flink batch job, because
pyiceberg 0.9.1 does not apply v2 deletes.

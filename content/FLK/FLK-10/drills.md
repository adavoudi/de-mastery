# FLK-10 · Interview drills

**1. "How does Flink turn a Debezium change stream into Iceberg table updates?"**
> The `debezium-json` format reads the Debezium envelopes and emits changelog RowKinds (c→+I,
> u→-U/+U, d→-D). An `INSERT … SELECT` into a format-version-2 Iceberg table with a primary key
> routes those into equality deletes + new data files keyed on the PK, so the table reflects the
> current source state.

**2. "What makes the pipeline exactly-once?"**
> Commit-on-checkpoint. The Iceberg sink buffers writes and commits one Iceberg snapshot per
> completed Flink checkpoint, idempotently per checkpoint id. On recovery Flink replays from the
> last checkpoint and the committer won't re-commit an already-committed snapshot, so no change is
> dropped or double-applied.

**3. "Why must the Iceberg table be v2 with a primary key for CDC?"**
> Applying updates and deletes needs row-level deletes, which only Iceberg v2 (merge-on-read with
> equality/position deletes) supports, and the primary key tells Flink which row an update or delete
> targets. A v1 table can only append.

**4. "The source schema gains a column. How do you evolve the target without downtime or a
rewrite?"**
> `ALTER TABLE … ADD (col TYPE)` on the Iceberg catalog. Iceberg schema evolution is metadata-only:
> existing data files are untouched, old rows read NULL for the new column, new rows carry it. Then
> start selecting the new column in the upsert.

**5. "You see small-file growth on the Iceberg table. Why, and what do you do?"**
> Each checkpoint commits a snapshot with new (often small) data/delete files; frequent checkpoints
> or low throughput make many small files. Mitigate with less frequent checkpoints, Iceberg's
> compaction/`rewrite_data_files`, and periodic snapshot expiration — the same maintenance you do on
> your lakehouse tables (LKH-05).

**6. "How do you test a CDC pipeline deterministically without a database?"**
> Feed a fixed file of Debezium-JSON envelopes through the `debezium-json` format (no Docker), run
> the upsert into a local Iceberg hadoop-catalog table, then reconcile the table against an
> independent fold of the same change events (apply c/u/d by key). The live Postgres→Debezium path
> is an extra lab layer, not the graded one.

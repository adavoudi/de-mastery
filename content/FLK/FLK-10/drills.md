# FLK-10 · Interview drills

**1. "How does Flink turn a Debezium change stream into Iceberg table updates?"**
> The `debezium-json` format reads the Debezium envelopes and emits changelog RowKinds (c→+I,
> u→-U/+U, d→-D). An `INSERT … SELECT` into a format-version-2 Iceberg table with a primary key
> sends those RowKinds into equality deletes and new data files, which use the PK. So the table shows the
> current source state.

**2. "What makes the pipeline exactly-once?"**
> Commit-on-checkpoint. The Iceberg sink buffers writes. It commits one Iceberg snapshot for each
> completed Flink checkpoint, idempotently per checkpoint id. On recovery, Flink replays from the
> last checkpoint. The committer does not commit again a snapshot that it already committed. So the sink does not
> drop any change and does not apply any change twice.

**3. "Why must the Iceberg table be v2 with a primary key for CDC?"**
> To apply updates and deletes, you need row-level deletes. Only Iceberg v2 (merge-on-read with
> equality/position deletes) supports them. The primary key tells Flink which row an update or delete
> targets. A v1 table can only append.

**4. "The source schema gains a column. How do you evolve the target without downtime or a
rewrite?"**
> Run `ALTER TABLE … ADD (col TYPE)` on the Iceberg catalog. Iceberg schema evolution is metadata-only.
> It does not change the existing data files. Old rows read NULL for the new column. New rows carry it. Then
> select the new column in the upsert.

**5. "You see small-file growth on the Iceberg table. Why, and what do you do?"**
> Each checkpoint commits a snapshot with new (often small) data and delete files. Frequent checkpoints
> or low throughput make many small files. To reduce them, use less frequent checkpoints, use Iceberg
> compaction (`rewrite_data_files`), and expire snapshots periodically. This is the same maintenance that you do on
> your lakehouse tables (LKH-05).

**6. "How do you test a CDC pipeline deterministically without a database?"**
> Feed a fixed file of Debezium-JSON envelopes through the `debezium-json` format (no Docker). Run
> the upsert into a local Iceberg hadoop-catalog table. Then reconcile the table against an
> independent fold of the same change events (apply c/u/d by key). The live Postgres→Debezium path
> is an extra lab layer. It is not the graded path.

# FLK-10 · CDC & lake sinks — Debezium changelog → Iceberg, commit on checkpoint, schema evolution

## Why Flink is the CDC-to-lakehouse engine

You already load your lakehouse: an operational database changes, and those changes have to land in
Iceberg so your dbt/Spark models can read a current, queryable table. The clean way to do that is
**Change Data Capture (CDC)**: the database's write-ahead log is turned into a stream of
**change events** (insert / update / delete), usually by **Debezium**, and a stream processor
**applies** those changes to a target table. Flink is purpose-built for this because:

- it understands **changelog semantics** natively — a Debezium event stream is just a changelog
  (`+I` / `-U` / `+U` / `-D`), the same RowKinds you met in FLK-04;
- its Iceberg sink **commits on checkpoint**, so the target table advances **atomically** and
  **exactly-once** — each Iceberg snapshot corresponds to a completed Flink checkpoint;
- it handles **schema evolution**: a new column in the source becomes a new Iceberg column, and the
  old rows read back `NULL` for it.

This is the SOX-grade loading path: no double-applied change, no dropped change, and a full snapshot
history on the target.

## How it works (objective dse-flink:cdc)

### 1. Read the change stream as a changelog

Flink ships the `debezium-json` format (part of `flink-json`, in the dist). A table declared with
`'format' = 'debezium-json'` interprets each Debezium envelope
(`{"before":…, "after":…, "op":"c|u|d"}`) and emits the right RowKinds:

```sql
CREATE TABLE cdc (
  id INT, name STRING, amt INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector'='filesystem', 'path'='…', 'format'='debezium-json');
```

- `op=c` → `+I`
- `op=u` → `-U` (old) then `+U` (new)
- `op=d` → `-D`

In production the source is `upsert-kafka` or the Flink CDC connectors (Debezium embedded) reading
the DB log directly; in this module the no-Docker graded path reads a **Debezium-JSON file** so the
whole pipeline is deterministic and needs no containers. The live Postgres→Debezium path is an
optional `streaming`-lab layer.

### 2. Define an upsert-capable Iceberg sink

To **apply** a changelog (not just append it) the Iceberg table must be **v2** (merge-on-read,
position/equality deletes) and have a **primary key** so Flink knows which row an update/delete
targets:

```sql
CREATE TABLE ice.db.customers (
  id INT, name STRING, amt INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('format-version' = '2');
```

Then the sink is just an `INSERT … SELECT` from the changelog table into the Iceberg table:

```sql
INSERT INTO ice.db.customers SELECT id, name, amt FROM cdc;
```

Flink routes `+U`/`-U`/`-D` into Iceberg equality deletes + new data files keyed on the primary
key. The result: the Iceberg table always reflects the **current** state of the source.

### 3. Commit on checkpoint (exactly-once)

The Iceberg sink **buffers writes and commits an Iceberg snapshot only when a Flink checkpoint
completes**. That is what makes the pipeline exactly-once: on recovery, Flink replays from the last
checkpoint and the sink's committer is idempotent per checkpoint id, so a snapshot is never
double-committed and never lost. You can see it on disk — the table's `metadata/` grows a new
`vN.metadata.json` and a `snap-*.avro` manifest list per committed checkpoint.

### 4. Schema evolution

When the source adds a column, evolve the target with an Iceberg `ALTER TABLE … ADD`:

```sql
ALTER TABLE ice.db.customers ADD (region STRING);
```

Iceberg schema evolution is **metadata-only** and **safe**: existing rows read back `NULL` for the
new column, new rows carry it, and no data is rewritten. This is the same column-add you rely on in
your Athena/Iceberg tables, driven here by the CDC pipeline.

## What the test asserts

- **Reconciliation (G6).** After applying the change events, the Iceberg table's contents must equal
  an independent **DuckDB** fold of the *same* events (apply c/u/d by primary key). Deleted keys are
  absent; updated keys show the latest values. The streaming result must equal the batch answer over
  the same changelog (TESTING.md streaming rule).
- **Commit on checkpoint.** The table has ≥1 Iceberg snapshot after the job completes (the commit
  happened); the data is visible only via that committed snapshot.
- **Schema evolution.** After `ALTER … ADD`, old rows read `NULL` for the new column and new rows
  carry its value — reconciled against the DuckDB fold that models the same evolution.

Nothing is graded on timing or on the exact SQL text (an alternate upsert phrasing passes).

## From your world

| Lakehouse loading | Flink CDC → Iceberg | Note |
|---|---|---|
| Nightly `MERGE` of a source extract | continuous changelog apply | no batch window; always current |
| Snapshot isolation of your Iceberg tables | commit on checkpoint = one snapshot per checkpoint | atomic, exactly-once |
| `ALTER TABLE ADD COLUMN` in Athena | `ALTER TABLE … ADD` on the Flink Iceberg catalog | metadata-only, old rows NULL |
| Dedup/latest-per-key staging | the sink applies updates/deletes by primary key | target = current state |

## When NOT to use this

- **Append-only event logs.** If you never update or delete, a plain append Iceberg sink (or Kafka →
  Iceberg) is simpler than a v2 upsert table with equality deletes.
- **Tiny, low-change dimensions.** A periodic batch `MERGE` (dbt/Spark) may be cheaper to operate
  than a standing Flink job.
- **No primary key available.** Upsert needs a key; without one you can only append, and dedup/SCD2
  must be modelled explicitly downstream.
- **Heavy historical backfills.** Bulk-load with batch first, then switch the CDC job on for the
  tail — streaming a full history through equality deletes is wasteful.

## Further reading

Official docs only (Flink 2.2 / Iceberg):

- Debezium format: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/debezium/>
- CDC connectors overview: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/overview/>
- Iceberg Flink connector (writes, upsert, schema): <https://iceberg.apache.org/docs/latest/flink-writes/>
- Iceberg schema evolution: <https://iceberg.apache.org/docs/latest/evolution/>

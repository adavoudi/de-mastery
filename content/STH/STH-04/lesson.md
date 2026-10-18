# STH-04 · Iceberg as streaming storage — commit interval vs small files

## Why it exists

You already run Iceberg as a *batch* lakehouse: dbt or Spark writes a snapshot every run, Athena
and Trino read it. A **streamhouse** keeps the same table but lets Flink write to it continuously
and lets readers see new data within seconds. Iceberg does not change — what changes is *who*
commits and *how often*. A Flink Iceberg sink commits a new snapshot **on every checkpoint**. The
checkpoint interval is therefore the single knob that decides your freshness *and* your file count,
and the two pull against each other.

This is the small-files problem you already know, but caused by time instead of by a bad
`repartition`. Each commit writes at least one data file per partition per writer task. Checkpoint
every second and you get ~60 snapshots and ~60 data files a minute per partition; readers then open
hundreds of tiny files and planning gets slow. Checkpoint every five minutes and files are fat and
few, but your "streaming" table is five minutes stale. There is no free lunch: **freshness and file
count are the same dial**, and compaction is the tax you pay to decouple them after the fact.

## How it works

A Flink → Iceberg pipeline has three moving parts:

1. **A bounded or unbounded source.** For CDC the no-Docker path is a `debezium-json` file source:
   each line is a change event (`op` = `c`/`u`/`d` with `before`/`after`). A **bounded** source lets
   the streaming job *finish* and fire its final commit — an unbounded `datagen`/`VALUES` source
   leaves the committer waiting and never commits (a classic hang; see "When NOT to use").
2. **The sink's commit cadence = the checkpoint cadence.** Each committed checkpoint becomes one
   Iceberg **snapshot**. One job run over a bounded input commits **one** snapshot and writes
   roughly **one data file per writer task per partition**. Run the ingest as *many small jobs*
   (one per micro-batch) and you get *many* snapshots and *many* small files; run it as *one job
   over a batched input* and you get one snapshot and one file. That relationship is deterministic
   and is exactly what this exercise measures.
3. **Upsert vs append.** A target with `PRIMARY KEY (...) NOT ENFORCED` and `'format-version'='2'`
   applies the changelog's `+I/-U/+U/-D` by key (row-level deletes); without the PK the sink can
   only append and the table never reflects deletes/updates.

![Many small jobs commit one snapshot and one tiny file each; one batched job commits one snapshot and one file](diagrams/commit-cadence.svg)

**Iceberg vs Paimon for the streaming write.** Both land on the lake and both commit per
checkpoint, so both suffer the small-files dial. The difference is the *merge model*: Iceberg v2 is
**copy-on-write-ish / merge-on-read** with equality or positional delete files — great for mostly-
append tables with occasional updates, but reads must merge deletes. Paimon is an **LSM tree** with
primary-key buckets and background compaction built for *high-rate upserts* — updates land as new
sorted runs and compaction merges them, so a changelog-heavy table stays read-efficient without you
scheduling a separate rewrite job. For a feed that is 90% appends, Iceberg is the simpler fit; for a
feed that is mostly updates to the same keys (a CDC mirror of an OLTP table), Paimon's LSM usually
wins on read amplification. This module writes the *same* CDC into both and compares the result.

## From your world

| Your batch Iceberg world | Streaming Iceberg (streamhouse) | Note |
|---|---|---|
| One snapshot per dbt/Spark run | One snapshot per Flink **checkpoint** | the interval is now the freshness SLA |
| Small files from a bad `repartition` | Small files from a short checkpoint interval | same symptom, time-driven cause |
| `OPTIMIZE` / `rewrite_data_files` nightly | Compaction decouples freshness from file size | still needed; it's the tax |
| `MERGE INTO dim USING cdc` | `INSERT INTO tgt SELECT … FROM debezium_src` by PK | the sink applies +I/-U/+U/-D |
| "Pick CoW or MoR for this table" | "Pick Iceberg v2 or Paimon LSM for this feed" | update rate decides |

## When NOT to use it

- **Don't stream into Iceberg with a tiny checkpoint interval and no compaction.** You will drown
  the table in small files and slow every reader. If you truly need sub-second freshness, that is a
  job for a stream-native store (Fluss / Kafka) in front, tiered to the lake less often.
- **Don't use an unbounded `VALUES`/`datagen` source into an Iceberg sink in a test or a one-shot
  backfill** — the committer never sees a final checkpoint and the job hangs with zero commits. Use
  a bounded `filesystem`/`debezium-json` source so the job finishes and commits.
- **Don't reach for Iceberg v2 MoR when the feed is almost entirely high-rate upserts to the same
  keys** — the delete-file overhead and read-side merge cost grow; Paimon's LSM is the better fit.
- **Don't stream at all if hourly freshness is fine.** A batch Iceberg write on a schedule is
  simpler, cheaper and produces naturally large files.

## Further reading

Official docs only:

- Apache Iceberg — Flink writes & the streaming sink / commit on checkpoint:
  <https://iceberg.apache.org/docs/latest/flink-writes/>
- Apache Iceberg — maintenance & compaction (`rewrite_data_files`, small files):
  <https://iceberg.apache.org/docs/latest/maintenance/>
- Apache Paimon — primary-key tables, LSM & compaction:
  <https://paimon.apache.org/docs/master/primary-key-table/overview/>
- Flink 2.2 — the `debezium-json` format (bounded CDC source):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/debezium/>

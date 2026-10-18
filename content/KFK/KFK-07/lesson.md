# KFK-07 — Kafka Connect & CDC

## Why it exists

Most of your source-of-truth data lives in operational databases, not in Kafka.
You could poll those tables on a schedule — but polling misses intermediate states
(a row that was inserted and deleted between two polls never appears), hammers the
OLTP system, and lags. **Change Data Capture (CDC)** reads the database's own
*transaction log* (the Postgres WAL) and emits one event per committed row change,
in commit order, with before/after images. **Kafka Connect** is the runtime that
hosts such connectors so you don't write bespoke ingestion code per source.

For an analytics engineer this is the missing half of SCD2. You already build
slowly-changing dimensions from daily snapshots in dbt; CDC gives you the *stream*
of row changes directly, so the dimension can be rebuilt continuously and no
intermediate state is lost.

## How it works

- **Workers, connectors, tasks.** A Connect *cluster* is a set of worker JVMs. A
  *connector* is a configured integration (e.g. Debezium Postgres); the connector
  splits its work into *tasks* that actually move data. Source connectors read
  external systems → Kafka; sink connectors read Kafka → external systems.
- **Converters** turn Connect's internal records into bytes on the wire (JSON,
  Avro+Schema Registry, Protobuf). **SMTs** (Single Message Transforms) tweak
  records in-flight (rename fields, route topics, extract the `after` image). A
  **DLQ** (dead-letter queue) captures records that fail conversion so one bad row
  doesn't halt the pipeline.
- **Debezium change-event envelope.** Each CDC message value is a record with:
  - `op` — the operation: `c` (create/insert), `u` (update), `d` (delete), `r`
    (read, emitted during the initial **snapshot** of existing rows).
  - `before` — the row image before the change (null for inserts/reads).
  - `after` — the row image after the change (null for deletes).
  - `source` — metadata (LSN, commit timestamp `ts_ms`, table, txId).
  A **tombstone** is a message with a non-null key and a **null value**, emitted
  after a delete so a compacted topic can drop the key entirely.
- **Snapshots.** When a connector first starts it emits `op=r` for every existing
  row (a consistent snapshot), then switches to streaming WAL changes. Your
  downstream logic must treat `r` like an initial insert.
- **Outbox pattern.** Rather than CDC-ing business tables directly, services write
  domain events to an `outbox` table in the same transaction; Debezium streams the
  outbox. This decouples the public event schema from the internal table shape.

## Deriving SCD2 from the change stream

Order the change events by commit time (and LSN as a tiebreaker) per key, then fold
them into dimension versions:

- `r`/`c` for a new key → open a new current version (`valid_from = ts`,
  `valid_to = null`, `is_current = true`).
- `u` on an existing key → **close** the current version (`valid_to = ts`,
  `is_current = false`) and **open** a new one with the `after` image.
- `d` → close the current version; emit no new open version (the entity is gone).
- A tombstone (null value) is a compaction marker — it carries no new state, so
  SCD2 derivation **skips** it (the preceding `d` already closed the row).

This is the SOX-friendly, fully-auditable version of a dimension: every state the
entity ever had, with exact validity windows, reconstructed from the OLTP log.

## From your world

- A Debezium stream of a `customers` table **is** the input to a dbt SCD2
  snapshot — except continuous and lossless, instead of a daily `dbt snapshot`.
- `op=r` (snapshot read) is the initial load; `c/u/d` are the incremental
  changes you'd otherwise diff between two daily extracts.
- Closing the old version on `u` and opening a new one is exactly `valid_from` /
  `valid_to` / `is_current` bookkeeping in a Kimball Type-2 dimension.
- The outbox pattern is the streaming analogue of exposing a curated staging model
  instead of raw source tables — you publish a contract, not your schema.

## When NOT to use it

- When a nightly batch is genuinely enough (a dimension that changes monthly does
  not need WAL-level CDC).
- When you can't enable logical replication on the source, or the DBA won't grant
  replication slots — CDC needs `wal_level=logical` and a slot, which retains WAL
  and can fill the disk if a consumer stalls.
- For high-churn tables where you only ever want the *latest* state and never the
  history — a compacted topic of `after` images (or a sink upsert) is simpler than
  full SCD2.

## Further reading

- Kafka — Connect concepts: https://kafka.apache.org/documentation/#connect
- Debezium 3.0 — Postgres connector (events, snapshots, tombstones): https://debezium.io/documentation/reference/3.0/connectors/postgresql.html
- Debezium — the change event structure: https://debezium.io/documentation/reference/3.0/connectors/postgresql.html#postgresql-events
- Debezium — the outbox event router: https://debezium.io/documentation/reference/3.0/transformations/outbox-event-router.html

# STH-03 · Apache Fluss — PK tables, lake tiering, streaming vs batch consistency

## Why Fluss exists

Kafka gives you a cheap, low-latency *log*, but a log is a poor place to answer "what is the current
value for key K?" or to run columnar OLAP — the log is row-oriented, append-only, and expensive to
retain. The lakehouse gives you cheap columnar OLAP, but only at batch latency. **Apache Fluss** is
a *streaming storage* layer built for exactly this gap: columnar, low-latency, and designed to be
**tiered to the lake** so the recent data lives in Fluss (sub-second) and the historical data lives
in Paimon/Iceberg (cheap, queryable) — **as one logical table**.

Fluss offers two table kinds:

- **Log tables** — append-only, like a Kafka topic but columnar; you read them as a stream.
- **Primary-key (PK) tables** — upsertable; the latest value per key is served with low latency,
  and you can read either the changelog (streaming) or a point-in-time snapshot (batch).

## How lake tiering makes one table serve two latencies

When you create a Fluss table `WITH ('table.datalake.enabled'='true')`, a **tiering service** (a
Flink job) continuously moves committed data from Fluss's hot store down into the lake format
(Paimon here). The crucial property is that the two tiers are **one logical table**:

- Read the Fluss table name directly → you get the **real-time** view (hot Fluss data merged with
  what has been tiered).
- Read the `` `table$lake` `` system table → you get the **tiered lake** view (what has landed in
  Paimon), readable in **batch** by Flink or by a pure-Python engine (pypaimon) with **no Flink at
  all**.

The invariant you must preserve — and the thing this module grades — is **read consistency**: once
the data has been tiered and quiesced, a **streaming** read of the table and a **batch** read of the
lake tier return the **same set of rows**. If they disagree, the streamhouse's whole promise ("one
copy, two latencies, same answer") is broken. This is the STH streaming-vs-batch oracle (TESTING.md
G6) applied to tiered storage.

### What has to be true for tiering to work

1. `'table.datalake.enabled'='true'` on the table (opt-in per table).
2. A Fluss **PK table** so there is a well-defined latest value per key to tier (`PRIMARY KEY(...)
   NOT ENFORCED`).
3. The server must know the lake format and warehouse (`datalake.format: paimon`,
   `datalake.paimon.warehouse: file://...`) — the fixture wires this when you pass
   `fluss_cluster(paimon_warehouse=...)`.
4. The `$lake` read path needs `paimon-flink` on the classpath (the fixture already provides it).

Because tiering is asynchronous, a batch read of `$lake` must be done with a **wall-clock deadline**
and bounded (never block on an unbounded stream) — the fixture's `paimon_sql.batch` / the Fluss
runner's bounded `$lake` read handle this; tiering typically lands in ~30–40 s.

## From your world

You already run two serving systems: a low-latency store (DynamoDB/ElastiCache) for the app's
point lookups, and Athena/Redshift over Iceberg for historical analytics — **two copies, kept in
sync by pipelines you own**. A Fluss PK table with lake tiering collapses that into **one logical
table**: the hot tier answers the sub-second lookup, the lake tier answers the historical scan, and
Fluss keeps them consistent for you. The read-consistency check in this exercise is the same thing
your reconciliation jobs assert today — that the fast store and the warehouse agree.

## When NOT to use it

- **You only ever need batch/OLAP.** If nothing reads the hot tier, skip Fluss and write straight to
  Paimon/Iceberg — fewer moving parts.
- **You only ever need a log.** If every consumer just replays events and nobody needs a keyed
  latest-value or OLAP, a plain Kafka/Fluss log table (or Kafka itself) is enough; the PK store and
  tiering are overhead.
- **Tiny data / low QPS.** A single database can serve both the lookup and the scan; a tiered
  streaming store is only worth its operational cost at scale or strict freshness SLAs.

## Further reading

- Apache Fluss — Introduction (streaming storage, log vs PK tables):
  <https://fluss.apache.org/docs/intro/>
- Apache Fluss — Lakehouse / tiering to Paimon:
  <https://fluss.apache.org/docs/maintenance/tiered-storage/lakehouse-storage/>
- Apache Paimon — Primary Key Table (the lake tier format):
  <https://paimon.apache.org/docs/master/concepts/primary-key-table/merge-engine/>

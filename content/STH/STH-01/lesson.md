# STH-01 · Architecture history — Lambda → Kappa → lakehouse → streamhouse

## Why these architectures exist

Every data architecture is an answer to one question: **how fresh must the answer be, and what
will you pay for that freshness?** The four landmark architectures below are four different answers,
and each exists because the previous one hurt somewhere.

- **Lambda** (Marz, ~2011). Run **two** pipelines over the same events: a *batch layer* that
  recomputes the correct, complete result slowly, and a *speed layer* that computes an approximate,
  low-latency result for the recent window. A serving layer merges them. It exists because the batch
  tools of the day could not give low latency and the streaming tools could not give completeness or
  reprocessing. The cost is that **you maintain the same business logic twice, in two code bases,
  and they drift** — the batch and speed answers disagree and someone has to reconcile them.

- **Kappa** (Kreps, ~2014). Delete the batch layer. Keep **one** streaming pipeline; treat the log
  (Kafka) as the system of record and *replay* it to reprocess history. One code path, no drift. It
  exists to kill Lambda's dual-code tax. The cost: **long-term, queryable history in Kafka is
  expensive**, ad-hoc OLAP over the log is awkward, and replaying months of events to fix a bug is
  slow and operationally heavy.

- **Lakehouse** (Databricks, ~2020). Put a **transactional table format** (Iceberg, Delta, Hudi) on
  cheap object storage, so one copy of the data serves both SQL/BI and ML, with ACID commits, time
  travel and schema evolution — warehouse semantics on a data lake. It exists to end the
  warehouse-plus-lake split (two storage systems, two copies, ETL between them). The cost that
  matters here: **it commits at batch/micro-batch latency** — snapshots land every few minutes at
  best, so it does not answer sub-minute streaming questions, and frequent commits create a
  small-files problem.

- **Streamhouse** (coined by Ververica, ~2024; now a cross-vendor working group — Aiven, Confluent,
  Redpanda, StreamNative, Ververica). Put a **stream-native storage layer** (Apache Paimon, Apache
  Fluss) on the lake so **one copy** of the data serves *streaming* reads (changelogs, CDC, low
  latency) **and** batch/OLAP reads, usually with Flink as the engine. It exists to give the
  lakehouse a sub-minute tier and the stream a cheap, queryable, long-term tier — Kappa's single
  pipeline but with lakehouse-grade storage underneath. The cost: **more moving parts and a newer,
  less settled ecosystem**; you pay for the extra tier in operations and skills.

The through-line: Lambda had two pipelines (drift); Kappa unified the pipeline but not storage
(expensive history); the lakehouse unified storage but at batch latency; the streamhouse unifies
storage **and** serves both latencies — at the price of a younger, more complex stack.

## How to classify an architecture (the rubric)

You classify a real system by its **storage and pipeline shape**, not by what the vendor calls it.
Four yes/no signals decide it, applied in order:

1. **Two parallel pipelines** (a batch recompute path *and* a separate speed path) that are
   merged at serving time → **Lambda**.
2. **One streaming pipeline** whose system of record and reprocessing substrate is an
   **append-only log** (Kafka/Pulsar), with no transactional lake table as the primary store →
   **Kappa**.
3. **One transactional lake table format** (Iceberg/Delta/Hudi) as the primary store, read by
   SQL/BI, committing at **batch/micro-batch** latency (minutes), with no stream-native storage →
   **Lakehouse**.
4. **A stream-native storage layer** (Paimon/Fluss, or Iceberg explicitly driven as streaming
   storage) that serves **both** a streaming/changelog read path **and** a batch/OLAP read path from
   **one copy** → **Streamhouse**.

The exercise encodes exactly these signals as booleans and asserts the classification is
deterministic.

## From your world

Your current stack is a **batch lakehouse**: dbt models materialized into Iceberg on S3, queried by
Athena/Redshift, refreshed on a schedule. Map the signals: you have one transactional lake format
(signal 3 → lakehouse), no second speed pipeline (not Lambda), Kafka is an *ingestion* bus feeding
batch loads rather than the queryable system of record (not Kappa), and nothing serves a sub-minute
changelog read from the same copy (not yet a streamhouse). A streamhouse would add Paimon/Fluss
**underneath** the same marts so the slowest-moving facts can also answer "what changed in the last
30 seconds" — without a parallel speed layer to reconcile (the SOX reconciliation pain of Lambda is
exactly what you want to avoid).

## When NOT to use a streamhouse

- **Hourly or daily freshness is fine.** A batch lakehouse is simpler, cheaper and more mature; do
  not add Paimon/Fluss, Flink and a tiering job to serve a dashboard that refreshes nightly.
- **You truly only need a log.** If every consumer is a streaming app replaying Kafka and nobody
  runs OLAP over history, plain Kappa is less to operate.
- **You need two genuinely different computations** (e.g. an exact nightly regulatory recompute and
  a separate real-time estimate with different logic). That is a legitimate Lambda, not a smell to
  "fix" by unifying.

## Further reading

- Apache Paimon — Overview (streaming lakehouse / streamhouse storage):
  <https://paimon.apache.org/docs/master/concepts/overview/>
- Apache Fluss — What is Fluss (streaming storage for real-time analytics):
  <https://fluss.apache.org/docs/intro/>
- Apache Iceberg — Flink streaming reads/writes (Iceberg as streaming storage):
  <https://iceberg.apache.org/docs/latest/flink/>

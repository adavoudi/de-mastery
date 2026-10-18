# KFK-01 · The log — events, state, retention and replay

## Why Kafka exists

You already live with a log, you just call it something else. An Iceberg table is a chain of
**snapshots**: each commit appends an immutable file set and advances a pointer. "The table as of
snapshot 42" is a *position* in an append-only history, and time-travel is reading the history up to
some position. Kafka takes that idea and makes it the primary interface rather than a side effect.

A Kafka **topic partition is an append-only, immutable, ordered log of records.** Producers append
to the end; each append gets a monotonically increasing **offset** (0, 1, 2, …). Consumers read
forward from an offset they choose. That is nearly the whole model, and almost everything else
(replication, consumer groups, exactly-once) is machinery to make that log durable and sharable.

The reason this is worth a whole system: it **decouples producers and consumers in time.** A
database row is *state* — the latest value, the old value overwritten and gone. A log keeps the
*events* that produced the state, in order, and keeps them around (retention). So a consumer that
was offline, or a brand-new consumer you write next year, can **replay** from any offset and rebuild
whatever view it needs. "Rebuild the orders table from scratch" stops being a scary migration and
becomes "read the log from offset 0 again."

## How the log works (objective ccdak:1 / ccaak:1)

- **Events vs. state.** An event is an immutable fact that happened: `order 7 set to $30 at 10:03`.
  State is a projection of the events: `the current total for account k`. The log stores events;
  any state is a **fold** (left-reduce) over the log. This is exactly how you'd rebuild an Iceberg
  dimension by replaying change rows, or how a dbt incremental model folds new rows into a table.
- **Offsets.** Within one partition, offset order **is** append order, and it never changes.
  `get_watermark_offsets(tp)` returns the current low (earliest retained) and high (next to be
  written) offsets. You can `assign` a partition at a specific offset and read forward — this is
  **replay from an offset**.
- **Replay from a timestamp.** You rarely know the offset you want; you know *"from 09:00 today."*
  `offsets_for_times([(topic, partition, ts_ms)])` returns, per partition, the **first offset whose
  record timestamp is ≥ ts_ms** (or the log end if none). Seek there and read forward. This is the
  log's version of "give me the table as of this wall-clock time."
- **Retention.** The log is not infinite. **Time retention** (`retention.ms`) and **size retention**
  (`retention.bytes`) delete the oldest *segments*. Separately, **log compaction**
  (`cleanup.policy=compact`) keeps only the latest record per key — turning the log into a
  materialized "current state per key", which is how `__consumer_offsets` and KTable changelogs work
  (KFK-08). Replay is only possible back to the earliest **retained** offset.
- **Keys and ordering.** A record may have a key. The partitioner sends all records with the same
  key to the same partition, so **per-key order is preserved** (ordering is only guaranteed *within*
  a partition, never across them). That is the hook the rest of the track hangs on.

## From your world

| Lakehouse / dbt / SOX | Kafka log | Note |
|---|---|---|
| Iceberg **snapshot id** | an **offset** (a position in history) | both are "the data as of a point in an append-only history" |
| Iceberg **time travel** to a snapshot | **replay** from an offset / timestamp | rebuild any view from the facts |
| dbt incremental **high-water mark** | the consumer's committed **offset** | "where did I get to last time" |
| Rebuilding a dimension by replaying change rows | folding the log into a KTable (KFK-08) | state = fold over events |
| SOX **completeness** (every fact accounted for) | read exactly `[low, high)` with no gaps | replay must cover every retained offset |

## When NOT to use Kafka

- **Request/response.** If a caller needs an answer *now* (an HTTP API, an RPC), that is not a log;
  use a service call. Kafka decouples in time — the opposite of request/response.
- **A simple task queue with a few workers.** If you just need "hand this job to one of N workers,
  ack it, forget it," SQS or RabbitMQ is simpler. Kafka's retention/replay/ordering is overhead you
  don't need.
- **Tiny data volumes / daily batch.** If a nightly `COPY`/`MERGE` already meets the SLA, a broker
  cluster is operational weight for no gain. Use the batch pipeline you already have.
- **As your system of record / database.** The log is great at history and streaming, but point
  lookups ("give me order 7 right now") belong in a database or a compacted KTable, not a linear scan
  of a topic.

## A note on what's coming (KIP-848, KIP-932)

Kafka 4.x ships the **new consumer group protocol (KIP-848)** — rebalancing moves coordination to
the broker and avoids the old "stop-the-world" rebalance; you'll meet it in KFK-04. **Share groups
(KIP-932)** add queue-like semantics (multiple consumers sharing a partition's records with
per-record acknowledgement) and are **early access** in this line — covered conceptually in KFK-04,
not graded here because the pinned client/broker support is preview-only.

## Further reading

Official docs only:

- Apache Kafka — Introduction (the log, topics, partitions, offsets):
  <https://kafka.apache.org/documentation/#intro_concepts_and_terms>
- Topic-level configs (`retention.ms`, `retention.bytes`, `cleanup.policy`):
  <https://kafka.apache.org/documentation/#topicconfigs>
- Log compaction:
  <https://kafka.apache.org/documentation/#compaction>
- Confluent — Kafka 101: Topics / Partitioning:
  <https://developer.confluent.io/courses/apache-kafka/topics/>

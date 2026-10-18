# KFK-08 — Kafka Streams

## Why it exists

Producers and consumers move events. They do not help you *transform* a stream.
Transform means join it, aggregate it, window it, or keep state.

You could build this yourself with a consumer, a database, manual offset handling,
and crash recovery. But then you would rebuild a hard distributed-systems problem.

**Kafka Streams** is a Java library (not a cluster). It turns a topology of stream
operations into a fault-tolerant, exactly-once application. The **state lives in
Kafka itself** (in changelog topics). When an instance crashes, a new instance
recovers the state by replaying the changelog.

For an analytics engineer, Kafka Streams is "dbt joins and aggregations, but
continuous and incremental." You can write the order-to-customer join as a dbt
model. In Kafka Streams, it becomes a streaming enrichment. It updates when either
side changes.

## How it works

- **KStream vs KTable.** A `KStream` is an **append-only event stream**. Each
  record is an independent fact (for example, an order placed). A `KTable` is a
  **changelog view of the latest value per key**. It is a dimension (for example,
  the current customer record). A `GlobalKTable` is a table that Kafka Streams
  fully replicates to every instance. You use it for lookups on topics that are
  not co-partitioned.
- **Joins.**
  - `KStream`–`KTable` (the join you will build): for each event, look up the
    current table value for its key and emit an enriched event. The stream drives
    the output. A table update alone emits nothing. This join requires
    **co-partitioning** (same key, same partition count).
  - `KStream`–`GlobalKTable`: look up by a key *derived* from the stream record.
    This join does not need co-partitioning.
  - `KStream`–`KStream`: windowed and symmetric.
- **State stores and changelogs.** Aggregations and joins keep local state in a
  **state store** (RocksDB by default). A compacted **changelog topic** backs the
  state store. This is how state survives a crash: Kafka Streams rebuilds the
  store from the changelog.

![Each input partition gets its own task and state store, and every store writes to a matching changelog partition](diagrams/streams-task-state.svg)

- **Windows.** Tumbling, hopping, sliding, and session windows group events by
  **event time**. They give time-bounded aggregations (for example, revenue per
  1-minute window).
- **EOS v2** (exactly-once semantics) puts consume, process, produce, and state
  update into one transaction. A failure does not double-count.
- **Testing without a cluster.** `TopologyTestDriver` runs a whole topology
  **in-process, with no broker and no RocksDB**. You send input records in and
  read output records out. The result is deterministic, in event time. This is the
  main way to unit-test Kafka Streams logic. This exercise also uses it for
  grading.

## From your world

- A `KTable` of customers **is a conformed dimension**. It has one current row per
  key, and updates change that row in place. A `KStream` of orders is the **fact
  stream** at order grain.
- The `KStream`–`KTable` join is the fact-to-dimension lookup that you write in a
  dbt staging or mart model. The difference is that it runs for each event. It
  uses the latest value of the dimension at the moment the event arrives.
- Co-partitioning (same key, same partition count on both sides) is the streaming
  version of making sure your join keys match. A mismatch silently drops or
  misroutes joins. This is like a bad grain in a Kimball model.
- A windowed revenue aggregation is a periodic snapshot fact table that Kafka
  Streams maintains continuously.

## When NOT to use it

- Do not use it when a periodic batch join in the warehouse is enough. Kafka
  Streams adds operational weight (state stores, changelogs, rebalancing). You do
  not need this weight for a nightly dimension.
- Do not use it in a team that works in several languages. Streams runs only on
  the JVM. If your team does not run Java services, Flink SQL or ksqlDB may fit
  better.
- Do not use it for heavy SQL-style analytics over many streams with complex
  joins. A SQL engine (Flink SQL) is often easier to use than hand-built
  topologies.

## Further reading

- Kafka — Streams concepts (KStream/KTable, joins, state): https://kafka.apache.org/documentation/streams/
- Kafka — Streams developer guide, joining: https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#joining
- Kafka — Testing Streams with TopologyTestDriver: https://kafka.apache.org/documentation/streams/developer-guide/testing.html
- Kafka — Streams architecture (state stores, changelogs, EOS): https://kafka.apache.org/documentation/streams/architecture

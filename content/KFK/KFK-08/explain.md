# Explain it back (KFK-08)

Explain to a dbt-fluent teammate: *how a Kafka Streams KStream-KTable join enriches
orders with customers, and why it isn't just "a join in SQL."*

Rubric — a good answer covers:

1. `KStream` = append-only facts (orders); `KTable` = latest value per key, a
   changelog (customers / a conformed dimension).
2. The **stream drives** the join: each order looks up the current customer; a
   table-only change emits nothing.
3. **Co-partitioning** (same key, same partition count) is required, or use a
   `GlobalKTable`.
4. State lives in a **state store** backed by a compacted **changelog topic**, so
   a crashed instance recovers by replaying it.
5. **Event-time** windows (not processing time) make aggregations correct under
   reordering.
6. **TopologyTestDriver** runs the topology in-process (no broker/RocksDB),
   deterministically in event time — the way the logic is unit-tested.
7. Analogy: fact→dimension lookup in dbt, but continuous and incremental, always
   reflecting the dimension's latest value at event arrival.

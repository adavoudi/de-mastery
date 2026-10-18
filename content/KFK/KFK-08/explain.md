# Explain it back (KFK-08)

Explain this to a teammate who knows dbt well. Cover *how a Kafka Streams
KStream-KTable join enriches orders with customers*. Also cover *why it is not
just "a join in SQL."*

Rubric — a good answer covers:

1. `KStream` = append-only facts (orders). `KTable` = latest value per key, a
   changelog (customers, a conformed dimension).
2. The **stream drives** the join. Each order looks up the current customer. A
   change in the table alone emits nothing.
3. **Co-partitioning** (same key, same partition count) is required. The other
   option is a `GlobalKTable`.
4. State lives in a **state store** that a compacted **changelog topic** backs. A
   crashed instance recovers by replaying the changelog.
5. **Event-time** windows (not processing time) keep aggregations correct when
   records arrive in a different order.
6. **TopologyTestDriver** runs the topology in-process (no broker, no RocksDB),
   deterministically in event time. This is how you unit-test the logic.
7. Analogy: it is the fact-to-dimension lookup in dbt, but continuous and
   incremental. It always uses the latest dimension value at the time the event
   arrives.

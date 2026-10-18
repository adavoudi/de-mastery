# Interview drills (KFK-08)

**1. Orders should be enriched with the customer's name. Which abstractions do you
use for each side, and why?**

Use `KStream` for orders (append-only facts, each order is independent). Use
`KTable` for customers (latest record per customer id, a changelog view = a
conformed dimension). A `KStream`-`KTable` join emits one enriched order for each
incoming order. It uses the customer value that is current at that moment.

**2. What triggers output in a KStream-KTable join — and what does NOT?**

An arriving **stream** record triggers a lookup and output. A **table** update
alone does **not** emit anything. It only changes what future stream records will
see. Because of this asymmetry, orders drive the enrichment.

**3. Your join silently produces no output for some orders. Name the most likely
cause.**

The topics are not co-partitioned. The orders and customers topics have different
partition counts, or they use different keys. Then matching keys do not share a
partition or task. Fix the key and the partition count, or use a `GlobalKTable`
for the lookup.

**4. How does a stateful Streams app recover after a crash without losing state?**

A compacted **changelog topic** backs each state store. After a restart, Kafka
Streams rebuilds the local RocksDB store by replaying the changelog. The same
happens when another instance takes the partition after a rebalance. Then
processing continues with the right state.

**5. Why test with TopologyTestDriver instead of a real broker?**

It runs the whole topology **in-process, no broker, no RocksDB**, with controlled
event time. Tests are fast and deterministic. They need no sleeps or containers.
You send inputs to an input topic and assert on an output topic. Use a
broker-based test only for an end-to-end smoke check.

**6. When would a GlobalKTable be the right call for the customer side?**

Use it when orders and customers are not co-partitioned (for example, orders are
keyed by order id, not customer id). The customer table must also be small enough
to replicate to every instance. A `KStream`-`GlobalKTable` join looks up by a key
*derived* from the order (the customer id). It has no co-partitioning
requirement.

# Interview drills (KFK-08)

**1. Orders should be enriched with the customer's name. Which abstractions do you
use for each side, and why?**

Orders → `KStream` (append-only facts, each order is independent). Customers →
`KTable` (latest record per customer id, a changelog view = a conformed dimension).
A `KStream`-`KTable` join emits an enriched order per incoming order, using the
customer value current at that moment.

**2. What triggers output in a KStream-KTable join — and what does NOT?**

An arriving **stream** record triggers a lookup and output. A **table** update
alone does **not** emit anything; it only changes what future stream records will
see. That asymmetry is exactly why orders drive the enrichment.

**3. Your join silently produces no output for some orders. Name the most likely
cause.**

Not co-partitioned: the orders and customers topics have different partition
counts or are keyed differently, so matching keys don't share a partition/task.
Fix the key and partition count, or use a `GlobalKTable` for the lookup.

**4. How does a stateful Streams app recover after a crash without losing state?**

Each state store is backed by a compacted **changelog topic**. On restart (or when
another instance picks up the partition after a rebalance), the local RocksDB store
is rebuilt by replaying the changelog, so processing resumes with the correct
state.

**5. Why test with TopologyTestDriver instead of a real broker?**

It runs the whole topology **in-process, no broker, no RocksDB**, with controlled
event time. Tests are fast, deterministic, and need no sleeps or containers — you
pipe inputs via an input topic and assert on an output topic. A broker-based test
is reserved for an end-to-end smoke check.

**6. When would a GlobalKTable be the right call for the customer side?**

When orders and customers aren't co-partitioned (e.g. orders keyed by order id, not
customer id) and the customer table is small enough to replicate to every instance.
A `KStream`-`GlobalKTable` join looks up by a key *derived* from the order
(the customer id) with no co-partitioning requirement.

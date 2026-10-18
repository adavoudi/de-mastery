# Interview drills (KFK-09)

**1. For a multi-company orders stream, what do you key by and why?**

Key by the **entity id** (`order_id`). All events for one order hash to one
partition. Kafka guarantees order within a partition. So the order gets a totally
ordered history (created before updated). Carry `company_id` as a field or header
for tenant filtering. Do not use it alone in the partitioning hash.

**2. A colleague keys by `company_id` "to keep each tenant together." What breaks
under real traffic?**

Tenants are skewed. The whole event volume of the biggest company hashes to a
single partition. This is a hot partition. It limits the throughput of that tenant
and increases its lag, while other partitions stay idle. Keying by the
high-cardinality entity id spreads the load of the big tenant across all
partitions. Each entity stays ordered.

**3. When do you choose a compacted entity topic over a time-retained event
topic?**

Choose it when consumers need the **latest state per key** and must be able to
rebuild it from the topic (a dimension or KTable source). Compaction keeps the most
recent value per key. Suppose consumers need the full **history** of what happened
(an audit or fact feed). Then use a time-retained event topic.

**4. Why can't you just bump the partition count later when you need more
parallelism?**

The key-to-partition mapping is `murmur2(key) % numPartitions`. If you increase
`numPartitions`, the keys map to new partitions. New events of a key can land on a
different partition than its existing data. This breaks per-key ordering across the
change. It also splits the history of a compacted key. Size the partitions for the
expected peak up front.

**5. Design the topic layout for orders and customers across many companies. State
the properties you're guaranteeing.**

- `orders`: a time-retained **event topic** keyed by `order_id`. It gives per-order
  ordering, and big tenants spread across partitions.
- `customers`: a **compacted entity topic** keyed by `customer_id`. It holds the
  latest state, and you can rebuild a KTable from it.
- Both topics carry `company_id` for tenant isolation and filtering.
- Size the partition count for the peak.
- Use one shared topic per domain, not one per tenant. This avoids the growth in
  metadata.

**6. How is choosing a Kafka key like choosing a fact-table grain?**

Both declare "one ordered unit of work." Keying by `order_id` means "one
partition-ordered sequence per order." This mirrors "one fact row per order." Pick
the wrong grain or key, and every downstream consumer inherits the ambiguity. It is
also expensive to change after data exists.

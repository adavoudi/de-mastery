# Interview drills (KFK-09)

**1. For a multi-company orders stream, what do you key by and why?**

Key by the **entity id** (`order_id`). All events for one order hash to one
partition, so Kafka's per-partition ordering guarantee gives the order a totally
ordered history (created before updated). Carry `company_id` as a field/header for
tenant filtering, but don't put it in the partitioning hash alone.

**2. A colleague keys by `company_id` "to keep each tenant together." What breaks
under real traffic?**

Tenants are skewed. The biggest company's entire event volume hashes to a single
partition — a hot partition that caps that tenant's throughput and inflates its lag,
while other partitions sit idle. Keying by the high-cardinality entity id spreads
the big tenant's load across all partitions while still ordering each entity.

**3. When do you choose a compacted entity topic over a time-retained event
topic?**

When consumers need the **latest state per key** and must be able to rebuild it from
the topic (a dimension / KTable source). Compaction keeps the most recent value per
key. If consumers need the full **history** of what happened (an audit/fact feed),
use a time-retained event topic.

**4. Why can't you just bump the partition count later when you need more
parallelism?**

key → partition is `murmur2(key) % numPartitions`. Increasing `numPartitions`
remaps keys, so a key's new events can land on a different partition than its
existing data — breaking per-key ordering across the change and splitting a
compacted key's history. Size partitions for projected peak up front.

**5. Design the topic layout for orders and customers across many companies. State
the properties you're guaranteeing.**

`orders` as a time-retained **event topic** keyed by `order_id` (per-order
ordering; big tenants spread across partitions). `customers` as a **compacted
entity topic** keyed by `customer_id` (latest state, KTable-rebuildable). Both
carry `company_id` for tenant isolation/filtering. Partition count sized for peak;
one shared topic per domain, not per tenant (avoids metadata explosion).

**6. How is choosing a Kafka key like choosing a fact-table grain?**

Both declare "one ordered unit of work." `order_id` keying = "one
partition-ordered sequence per order," mirroring "one fact row per order." Pick the
wrong grain/key and every downstream consumer inherits the ambiguity, and it's
expensive to change after data exists.

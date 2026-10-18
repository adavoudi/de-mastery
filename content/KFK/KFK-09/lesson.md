# KFK-09 — Event & topic design

## Why it exists

Topic and key design is the schema-modeling of streaming. Get it wrong and no
amount of consumer code fixes it: you can't change a topic's partition count
without reshuffling every key's ordering, and you can't retrofit a key choice that
preserves per-entity order. These decisions are as consequential — and as hard to
reverse — as choosing the grain of a fact table. For a multi-company (multi-tenant)
commerce platform, three properties must hold *by construction*:

- **Per-entity ordering.** All events for the same business entity (an order, a
  customer) must land on the **same partition**, because Kafka only guarantees
  order *within a partition*. If an order's `created` and `updated` events split
  across partitions, a consumer can see the update before the create.
- **Tenant isolation.** One company's events must not depend on another's for
  correctness, and ideally a noisy tenant shouldn't be able to reorder or starve
  another's stream.
- **No hot partition under skew.** Real tenants are skewed (one giant company, many
  tiny ones). A key that sends a whole big tenant to one partition creates a hot
  spot that caps throughput and lag for everyone on it.

## How it works

- **The key decides the partition.** The default partitioner computes
  `murmur2(key) % numPartitions` (null key → sticky/round-robin). So the key *is*
  the ordering and co-location contract. Choosing the key is the single most
  important design decision.
- **Key for per-entity order.** Key by the **entity id** whose order you must
  preserve (e.g. `order_id`). Every event for that entity hashes to one partition,
  so they are totally ordered.
- **Multi-tenancy choices.**
  - *Shared topic, keyed by entity:* one `orders` topic for all companies, keyed by
    `order_id`. Simple, but a per-tenant hot entity can still hot-spot, and tenant
    isolation is only logical.
  - *Composite key `(company_id, entity_id)`:* preserves per-entity order and keeps
    a tenant's entities together-ish, but keying by `company_id` alone would send a
    whole big tenant to one partition — a hot partition. The fix is to hash the
    **entity id** (high cardinality) into the partition while still carrying the
    `company_id` for routing/filtering, so a big tenant's load spreads across
    partitions yet each entity stays ordered.
  - *Topic-per-tenant:* strong isolation, but explodes partition/topic counts and
    doesn't scale to thousands of tenants.
- **Event vs entity topics.** An **event topic** is append-only facts
  (`order-placed`), retained by time — your fact stream. An **entity topic** is a
  **compacted** changelog keyed by entity id, retaining the latest state per
  key — your dimension/KTable source. Choose by whether consumers need history
  (event) or current state (entity/compacted).
- **Compaction** keeps the last value per key forever (plus optional time
  retention), which is why a compacted entity topic can rebuild a KTable from
  scratch.
- **Partition count** sets max consumer parallelism and is painful to increase
  (it changes key→partition mapping, breaking existing per-key order). Size for
  future throughput up front.

## From your world

- Choosing the key is choosing the **fact grain**: `order_id` keying is "one
  partition-ordered sequence per order," exactly like declaring the grain of a
  fact table is "one row per order line."
- The `company_id` dimension in a **multi-company mart** reappears here as the
  tenant attribute you carry on every event and must not let bleed across tenants.
- A compacted entity topic is a **conformed dimension** materialized on the log;
  an event topic is the **fact** feed.
- Avoiding a hot partition under tenant skew is the streaming version of avoiding a
  skewed join key or a badly chosen distribution key in Redshift.

## When NOT to use (these designs)

- **Don't key by `company_id` alone** when tenants are skewed — it guarantees a hot
  partition for the biggest tenant. Key by the high-cardinality entity id.
- **Don't use a null key** for events that need per-entity order — round-robin
  partitioning destroys ordering.
- **Don't use topic-per-tenant** at large tenant counts — the partition/metadata
  explosion hurts the whole cluster.
- **Don't over-partition** "just in case": every partition has fixed overhead
  (open files, replication, controller metadata), and you can't easily reduce the
  count later.

## Further reading

- Kafka — Topics, partitions, and the log: https://kafka.apache.org/documentation/#intro_concepts_and_terms
- Kafka — Log compaction: https://kafka.apache.org/documentation/#compaction
- Kafka — Producer partitioning / DefaultPartitioner: https://kafka.apache.org/documentation/#producerconfigs_partitioner.class
- Confluent — Multi-tenancy and topic design guidance: https://developer.confluent.io/courses/architecture/data-contracts/

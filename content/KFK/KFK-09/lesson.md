# KFK-09 — Event & topic design

## Why it exists

Topic and key design is the schema modeling of streaming. If you get it wrong,
consumer code cannot fix it. You cannot change the partition count of a topic
without reshuffling the key order. You also cannot add a key choice later that
keeps per-entity order. These decisions have large effects and are hard to
reverse. They are like choosing the grain of a fact table.

A multi-company (multi-tenant) commerce platform needs three properties. They must
hold *by construction*:

- **Per-entity ordering.** All events for the same business entity (an order, a
  customer) must land on the **same partition**. Kafka guarantees order only
  *within a partition*. Suppose the `created` and `updated` events of an order go
  to different partitions. Then a consumer can see the update before the create.
- **Tenant isolation.** The events of one company must not depend on the events of
  another company for correctness. Ideally, a noisy tenant cannot reorder or
  starve the stream of another tenant.
- **No hot partition under skew.** Real tenants are skewed (one giant company, many
  tiny ones). Suppose a key sends a whole big tenant to one partition. That
  partition becomes a hot spot. It limits throughput and increases lag for everyone
  on that partition.

## How it works

- **The key decides the partition.** The default partitioner computes
  `murmur2(key) % numPartitions` (null key → sticky/round-robin). So the key *is*
  the contract for ordering and co-location. Choosing the key is the most important
  design decision.
- **Key for per-entity order.** Use the **entity id** whose order you must keep
  (for example, `order_id`) as the key. Every event for that entity hashes to one
  partition, so the events are totally ordered.
- **Multi-tenancy choices.**
  - *Shared topic, keyed by entity:* one `orders` topic for all companies, keyed by
    `order_id`. It is simple. But a hot entity in one tenant can still create a hot
    spot. Tenant isolation is only logical.
  - *Composite key `(company_id, entity_id)`:* it keeps per-entity order. It also
    keeps the entities of a tenant together-ish. But a key of `company_id` alone
    would send a whole big tenant to one partition. That is a hot partition. The
    fix is to hash the **entity id** (high cardinality) to choose the partition.
    You still carry the `company_id` for routing and filtering. Then the load of a
    big tenant spreads across partitions, and each entity stays ordered.
  - *Topic-per-tenant:* it gives strong isolation. But the number of partitions and
    topics grows very large. It does not scale to thousands of tenants.
- **Event vs entity topics.** An **event topic** holds append-only facts
  (`order-placed`). Kafka retains it by time. It is your fact stream. An **entity
  topic** is a **compacted** changelog keyed by entity id. It retains the latest
  state per key. It is your dimension or KTable source. Choose by what consumers
  need: history (event topic) or current state (entity/compacted topic).
- **Compaction** keeps the last value per key forever (plus optional time
  retention). For this reason, a compacted entity topic can rebuild a KTable from
  scratch.
- **Partition count** sets the maximum consumer parallelism. It is painful to
  increase, because it changes the key-to-partition mapping. That breaks existing
  per-key order. Size it up front for future throughput.

## From your world

- Choosing the key is choosing the **fact grain**. Keying by `order_id` means "one
  partition-ordered sequence per order." Declaring the grain of a fact table means
  "one row per order line." The two choices work the same way.
- In a **multi-company mart**, you have a `company_id` dimension. Here it comes
  back as the tenant attribute. You carry it on every event. You must not let it
  bleed across tenants.
- A compacted entity topic is a **conformed dimension** materialized on the log.
  An event topic is the **fact** feed.
- Avoiding a hot partition under tenant skew is the streaming version of avoiding a
  skewed join key. It is also like avoiding a badly chosen distribution key in
  Redshift.

## When NOT to use (these designs)

- **Don't key by `company_id` alone** when tenants are skewed. It guarantees a hot
  partition for the biggest tenant. Key by the high-cardinality entity id.
- **Don't use a null key** for events that need per-entity order. Round-robin
  partitioning destroys the order.
- **Don't use topic-per-tenant** at large tenant counts. The growth in partitions
  and metadata hurts the whole cluster.
- **Don't over-partition** "just in case." Every partition has a fixed overhead
  (open files, replication, controller metadata). You also cannot easily reduce the
  count later.

## Further reading

- Kafka — Topics, partitions, and the log: https://kafka.apache.org/documentation/#intro_concepts_and_terms
- Kafka — Log compaction: https://kafka.apache.org/documentation/#compaction
- Kafka — Producer partitioning / DefaultPartitioner: https://kafka.apache.org/documentation/#producerconfigs_partitioner.class
- Confluent — Multi-tenancy and topic design guidance: https://developer.confluent.io/courses/architecture/data-contracts/

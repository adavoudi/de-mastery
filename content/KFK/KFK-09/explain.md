# Explain it back (KFK-09)

Explain to a teammate who knows Kimball well: *how you design topics and keys for a
multi-company commerce platform. Ordering, isolation, and load must all hold.*

Rubric — a good answer covers:

1. Kafka guarantees order only **within a partition**. The key decides the
   partition (`murmur2(key) % n`). So the key is the ordering contract.
2. Use the **high-cardinality entity id** (for example, `order_id`) as the key. This
   keeps the events of each entity in order on one partition.
3. **Don't key by `company_id`** alone. Skewed tenants create a **hot partition**.
4. Carry `company_id` for routing and filtering (tenant isolation). Hash the entity
   id for placement.
5. **Event topic** (time-retained facts) vs **entity topic** (compacted changelog
   of latest state). Choose by the need: history or current state.
6. The partition count limits parallelism. It is painful to change because it
   remaps keys. So size it up front. Topic-per-tenant does not scale to thousands
   of tenants.
7. Analogy: choosing the key = choosing the **fact grain**. The compacted entity
   topic = a conformed dimension on the log.

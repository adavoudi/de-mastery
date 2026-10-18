# Explain it back (KFK-09)

Explain to a Kimball-fluent teammate: *how you design topics and keys for a
multi-company commerce platform so ordering, isolation and load all hold.*

Rubric — a good answer covers:

1. Ordering is guaranteed only **within a partition**, and **key → partition**
   (`murmur2(key) % n`), so the key is the ordering contract.
2. Key by the **high-cardinality entity id** (e.g. `order_id`) to keep each
   entity's events ordered on one partition.
3. **Don't key by `company_id`** alone — skewed tenants create a **hot partition**.
4. Carry `company_id` for routing/filtering (tenant isolation) but hash the entity
   id for placement.
5. **Event topic** (time-retained facts) vs **entity topic** (compacted changelog
   of latest state) — chosen by history vs current-state needs.
6. Partition count caps parallelism and is painful to change (remaps keys), so size
   up front; topic-per-tenant doesn't scale to thousands.
7. Analogy: choosing the key = choosing the **fact grain**; the compacted entity
   topic = a conformed dimension on the log.

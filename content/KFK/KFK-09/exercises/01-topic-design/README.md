# Design topics for multi-company commerce

Design the topic and key layout for a multi-company (multi-tenant) commerce
platform. Then implement the partitioning function that makes your design hold.
Two checks grade you. A **rubric** checks your design decisions. A **property
test** checks your key and partition function (per-entity ordering, tenant
isolation, no hot partition under skew).

An `event` is a dict:

```python
{"company_id": "C001", "order_id": "O-123", "event_type": "created", "ts": 170...}
```

Companies are **skewed**: a few huge tenants, many tiny ones.

## Your task — implement `solution.py`

### 1. `message_key(event) -> str`

Return the Kafka message key for an order event. The key must make all events for
the **same order** go to one partition. Then the per-partition ordering of Kafka
keeps `created` before `updated`. The key must **not** send a whole big tenant to
one partition.

### 2. `partition_for(event, num_partitions) -> int`

Return the partition index in `range(num_partitions)` for the event. It must be a
**pure, deterministic** function of `message_key(event)` and `num_partitions`. The
same inputs give the same partition, across processes. Use a stable hash
(`hashlib`). Do not use `hash()` of Python, because Python salts it for each
process.

### 3. `design() -> dict`

Return your design decisions as a dict with exactly these keys:

```python
{
  "orders": {
     "topic_type": "event" | "entity",
     "retention":  "time"  | "compacted",
     "key":        "order_id" | "company_id" | "customer_id" | "random",
     "factors":    [ ... ],   # from the vocabulary below
  },
  "customers": {
     "topic_type": ...,
     "retention":  ...,
     "key":        ...,
     "factors":    [ ... ],
  },
}
```

Controlled vocabulary for `factors`:
`per_entity_ordering`, `tenant_isolation`, `avoid_hot_partition`,
`high_cardinality_key`, `latest_state_per_key`, `rebuildable_from_log`,
`append_only_history`, `time_retention`, `log_compaction`.

The defensible design:

- **orders** = a time-retained **event** topic (append-only history of order
  facts), keyed by `order_id`. Required factors: `per_entity_ordering`,
  `avoid_hot_partition`, `high_cardinality_key`. Also add at least one of
  `append_only_history` / `time_retention`.
- **customers** = a **compacted entity** topic (latest state per customer),
  keyed by `customer_id`. Required factors: `latest_state_per_key`,
  `log_compaction`. Also add at least one of `rebuildable_from_log` /
  `tenant_isolation`.

## Grading

- The **rubric** checks the `design()` choices and the required factor keys.
- **Property tests** (Hypothesis) check your `message_key` and `partition_for`:
  1. *Per-entity ordering* — all events for one `order_id` map to one partition.
  2. *Determinism* — the same event gives the same partition, recomputed in a fresh
     process.
  3. *No hot partition under skew* — for a heavily skewed tenant with many orders,
     the events spread across (nearly) all partitions, not just one.
  4. *Tenant isolation* — the key is derived from the entity. The tenant alone does
     not force the distinct orders of two tenants to collide on one partition.

Pure Python, no Docker.

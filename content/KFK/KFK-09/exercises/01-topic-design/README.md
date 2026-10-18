# Design topics for multi-company commerce

Design the topic + key layout for a multi-company (multi-tenant) commerce platform
and implement the partitioning function that makes your design hold. You are graded
by a **rubric** (your design decisions) **and a property test** on your key /
partition function (per-entity ordering, tenant isolation, no hot partition under
skew).

An `event` is a dict:

```python
{"company_id": "C001", "order_id": "O-123", "event_type": "created", "ts": 170...}
```

Companies are **skewed**: a few huge tenants, many tiny ones.

## Your task — implement `solution.py`

### 1. `message_key(event) -> str`

Return the Kafka message key for an order event. It must make all events for the
**same order** co-locate on one partition (so Kafka's per-partition ordering keeps
`created` before `updated`), while **not** sending a whole big tenant to one
partition.

### 2. `partition_for(event, num_partitions) -> int`

Return the partition index in `range(num_partitions)` for the event. It must be a
**pure, deterministic** function of `message_key(event)` and `num_partitions`
(same inputs → same partition, across processes). Use a stable hash
(`hashlib`), not Python's per-process-salted `hash()`.

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
  `avoid_hot_partition`, `high_cardinality_key`; plus at least one of
  `append_only_history` / `time_retention`.
- **customers** = a **compacted entity** topic (latest state per customer),
  keyed by `customer_id`. Required factors: `latest_state_per_key`,
  `log_compaction`; plus at least one of `rebuildable_from_log` /
  `tenant_isolation`.

## Grading

- The **rubric** checks the `design()` choices and required factor keys.
- **Property tests** (Hypothesis) check your `message_key` / `partition_for`:
  1. *Per-entity ordering* — all events for one `order_id` map to one partition.
  2. *Determinism* — same event → same partition, recomputed in a fresh process.
  3. *No hot partition under skew* — for a heavily skewed tenant with many orders,
     the events spread across (nearly) all partitions, not just one.
  4. *Tenant isolation* — the key is derived from the entity, so two tenants'
     distinct orders are not forced to collide onto one partition by tenant alone.

Pure Python, no Docker.

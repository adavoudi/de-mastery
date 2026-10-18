# Interview drills — FND-05

### 1. "How would you design for exactly-once?"
**Model answer.** I would not try to make the network deliver exactly once. That is impossible
over an unreliable channel (two generals). I would use at-least-once delivery, so that nothing is
ever lost. Then I would make the *effect* happen once at the sink. There are two levers, and I can
use one or both:

- Idempotent writes keyed on a stable unique id (`UPSERT ON CONFLICT DO NOTHING`, Iceberg
  `MERGE INTO`). Replays are then no-ops.
- Binding the write and the offset commit into one atomic transaction (Kafka transactional
  producer, Flink 2-phase-commit sink). Then I never ack consumption without durably writing the
  effect.

The hard part is the choice of the dedup key. It must be stable across retries and unique at the
true grain of the event.

### 2. "At-least-once vs exactly-once — when is at-least-once enough?"
**Model answer.** At-least-once is enough when the downstream operation is naturally idempotent.
Examples are an upsert keyed on a primary key, a `SET` of a value, or a MERGE. In these cases the
duplicates collapse on their own, and you avoid the cost of transactions.

You need true transactional exactly-once only when the effect is non-idempotent and you cannot
key it. Examples are appending to a ledger or incrementing a counter. Then you must prevent the
duplicate from ever being applied.

### 3. "A pipeline double-counts revenue on reprocessing. Diagnose and fix."
**Model answer.** This is an accuracy failure from non-idempotent application. The cause is
almost certainly a bare INSERT with no dedup key. Re-delivered or reprocessed events add rows
again.

Fix:

1. Identify the natural key of the event. For order items, it is `(company_id, order_item_id)`.
2. Dedup on it (keep first / `DISTINCT` / UPSERT).
3. Make the load a MERGE on that key, so that a re-run is a no-op.

Verify with the idempotency property: load twice and assert that the target is identical. A dbt
incremental model with a `unique_key` behaves exactly like this.

### 4. "Why can't you just trust 'exactly-once' from the framework?"
**Model answer.** The guarantee is end-to-end. It is not a checkbox. Kafka's transactional
producer gives exactly-once *within Kafka*. But suppose your sink write and the Kafka offset
commit are not in the same atomic unit. Then a crash in between still gives a duplicate or a loss.

The framework can guarantee exactly-once only when the sink participates. The sink must be
idempotent by key, or transactional with 2-phase commit. So I always ask: what is the dedup key or
the transaction boundary at the sink? Without one, the config only states a hope.

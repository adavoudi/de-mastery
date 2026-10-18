# Interview drills — FND-05

### 1. "How would you design for exactly-once?"
**Model answer.** I wouldn't try to make the network deliver exactly once — that's impossible over
an unreliable channel (two generals). I'd use at-least-once delivery so nothing is ever lost, then
make the *effect* happen once at the sink. Two levers: idempotent writes keyed on a stable unique
id (`UPSERT ON CONFLICT DO NOTHING`, Iceberg `MERGE INTO`), so replays are no-ops; and/or binding
the write and the offset commit into one atomic transaction (Kafka transactional producer, Flink
2-phase-commit sink) so I never ack consumption without durably writing the effect. The hard part
is choosing the dedup key: stable across retries, unique at the event's true grain.

### 2. "At-least-once vs exactly-once — when is at-least-once enough?"
**Model answer.** At-least-once is enough whenever the downstream operation is naturally idempotent
— an upsert keyed on a primary key, a `SET` of a value, a MERGE. In those cases duplicates collapse
on their own and you avoid the cost of transactions. You only need true transactional exactly-once
when the effect is non-idempotent (e.g. appending to a ledger, incrementing a counter) and you
can't key it, so you must prevent the duplicate from ever being applied.

### 3. "A pipeline double-counts revenue on reprocessing. Diagnose and fix."
**Model answer.** It's an accuracy failure from non-idempotent application — almost certainly a bare
INSERT with no dedup key, so re-delivered or reprocessed events add rows again. Fix: identify the
event's natural key (for order items, `(company_id, order_item_id)`), dedup on it (keep first /
`DISTINCT` / UPSERT), and make the load a MERGE on that key so a re-run is a no-op. Verify with the
idempotency property: load twice and assert the target is identical. This is exactly how a dbt
incremental model with a `unique_key` behaves.

### 4. "Why can't you just trust 'exactly-once' from the framework?"
**Model answer.** Because the guarantee is end-to-end, not a checkbox. Kafka's transactional
producer gives exactly-once *within Kafka*, but if your sink write and the Kafka offset commit
aren't in the same atomic unit, a crash in between still yields a duplicate or a loss. The
framework can only guarantee exactly-once when the sink participates — idempotent by key, or
transactional with 2-phase commit. So I always ask: what's the dedup key or the transaction
boundary at the sink? Without one, the config is aspirational.

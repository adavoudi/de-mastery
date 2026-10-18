# KFK-05 · Interview drills

**1. "Exactly-once in Kafka — what are the moving parts?"**
> Idempotent producer (no retry dups) + a transactional producer with a stable transactional.id that
> commits output writes AND the input offsets atomically (send_offsets_to_transaction) + downstream
> consumers reading with isolation.level=read_committed so aborted/in-flight writes stay invisible.

**2. "Why commit the consumer offsets inside the transaction instead of separately?"**
> So consume and produce are atomic. If you committed offsets separately, a crash between the output
> write and the offset commit would reprocess the input and duplicate the output. Inside the
> transaction, both the output and 'input consumed' commit or roll back together.

**3. "What is zombie fencing and what breaks it?"**
> A zombie is a presumed-dead producer still alive and committing. Kafka fences it via
> transactional.id + epoch: a new init_transactions bumps the epoch and the broker rejects the old
> epoch (ProducerFenced). A random per-run transactional.id breaks fencing — the two instances have
> different ids, so neither fences the other and both can commit (duplicates).

**4. "A read_committed consumer reads a topic that has an in-progress transaction. What does it
see?"**
> Nothing from that transaction until it commits — read_committed only surfaces committed records,
> and it won't advance past an open transaction's records (the last stable offset), so it waits
> rather than exposing uncommitted data.

**5. "When would you NOT reach for transactions?"**
> When a downstream idempotent upsert (by business key) already dedups, at-least-once is simpler and
> lower-latency. Transactions pay off specifically for atomic Kafka-to-Kafka read-process-write where
> no external dedup exists.

**6. "abort_transaction was called. What is guaranteed?"**
> All-or-nothing: none of that transaction's produced records are visible to read_committed
> consumers, and the input offsets it would have committed are not committed, so the input is
> reprocessed cleanly next time. No partial state leaks.

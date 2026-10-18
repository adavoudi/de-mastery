# KFK-05 · Interview drills

**1. "Exactly-once in Kafka — what are the moving parts?"**
> - An idempotent producer (no duplicates from retries).
> - A transactional producer with a stable transactional.id. It commits the output writes AND the
>   input offsets atomically (send_offsets_to_transaction).
> - Downstream consumers that read with isolation.level=read_committed. Aborted and in-flight writes
>   stay invisible to them.

**2. "Why commit the consumer offsets inside the transaction instead of separately?"**
> So that consume and produce are atomic. Suppose you commit the offsets separately. A crash between
> the output write and the offset commit reprocesses the input and duplicates the output. When the
> offsets are inside the transaction, the output and 'input consumed' commit together or roll back
> together.

**3. "What is zombie fencing and what breaks it?"**
> A zombie is a producer that people think is dead, but it is still alive and commits. Kafka fences
> it with transactional.id + epoch. A new init_transactions raises the epoch, and the broker rejects
> the old epoch (ProducerFenced). A random transactional.id for each run breaks fencing. The two
> instances have different ids, so neither fences the other, and both can commit (duplicates).

**4. "A read_committed consumer reads a topic that has an in-progress transaction. What does it
see?"**
> It sees nothing from that transaction until the transaction commits. read_committed shows only
> committed records. It does not advance past the records of an open transaction (the last stable
> offset). So it waits and does not expose uncommitted data.

**5. "When would you NOT reach for transactions?"**
> Suppose a downstream idempotent upsert (by business key) already removes duplicates. Then
> at-least-once is simpler and has lower latency. Transactions pay off for an atomic Kafka-to-Kafka
> read-process-write where no external dedup exists.

**6. "abort_transaction was called. What is guaranteed?"**
> All-or-nothing. No read_committed consumer sees any record that the transaction produced. The
> transaction does not commit the input offsets that it would have committed. So the next run
> reprocesses the input cleanly. No partial state leaks.

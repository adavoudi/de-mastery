# KFK-04 · Interview drills

**1. "At-most-once vs at-least-once vs exactly-once — one line each on how you achieve them."**
> At-most-once: commit before processing (or auto-commit) — a crash skips in-flight records.
> At-least-once: commit after processing — a crash reprocesses, never loses. Exactly-once: a
> transactional read-process-write with offsets committed inside the transaction (KFK-05), or
> at-least-once + an idempotent sink.

**2. "Auto-commit seems convenient. Why avoid it for important data?"**
> It commits on a timer regardless of whether you finished processing the polled records. A crash
> between the commit and finishing the work advances the offset past unprocessed records, losing
> them. Disable it and commit explicitly after the work is durably done.

**3. "A consumer dies mid-stream. What guarantees the next consumer in the group doesn't skip
records?"**
> The committed offset only advanced past records already processed-and-committed. The resuming
> consumer (same group.id) starts from that committed offset, so anything not yet committed is
> re-read. No gap, possibly a few duplicates.

**4. "How do you detect a consumer group falling behind?"**
> Monitor lag = log-end-offset − committed-offset per partition. Steadily rising lag means the group
> can't keep up; alert on it and scale out consumers (up to the partition count).

**5. "You add a 4th consumer to a group reading a 3-partition topic. What happens?"**
> A rebalance assigns the 3 partitions across consumers; with 3 partitions and 4 consumers, one
> consumer gets nothing (parallelism is capped at the partition count). Removing a consumer
> reassigns its partitions to the others.

**6. "At-least-once gives duplicates. How do you keep the warehouse correct?"**
> Make the sink idempotent: upsert by a business key (your SCD2/MERGE habit), or key the dedup on a
> unique event id. The duplicate overwrites itself to the same state. If no dedup is possible, use
> transactions for exactly-once.

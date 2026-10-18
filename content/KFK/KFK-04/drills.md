# KFK-04 · Interview drills

**1. "At-most-once vs at-least-once vs exactly-once — one line each on how you achieve them."**
> - At-most-once: commit before processing (or use auto-commit). A crash skips the in-flight
>   records.
> - At-least-once: commit after processing. A crash reprocesses and never loses records.
> - Exactly-once: a transactional read-process-write, with the offsets committed inside the
>   transaction (KFK-05). Another way is at-least-once plus an idempotent sink.

**2. "Auto-commit seems convenient. Why avoid it for important data?"**
> It commits on a timer, even if you did not finish processing the polled records. Suppose a crash
> happens between the commit and the end of the work. Then the offset is already past unprocessed
> records, and you lose them. Disable auto-commit. Commit explicitly after the work is durably done.

**3. "A consumer dies mid-stream. What guarantees the next consumer in the group doesn't skip
records?"**
> The committed offset moves only past records that are already processed and committed. The
> resuming consumer (same group.id) starts from that committed offset. So it re-reads everything
> that is not yet committed. There is no gap, but there can be a few duplicates.

**4. "How do you detect a consumer group falling behind?"**
> Monitor lag = log-end-offset − committed-offset for each partition. Lag that rises steadily means
> that the group cannot keep up. Alert on it and scale out the consumers (up to the partition
> count).

**5. "You add a 4th consumer to a group reading a 3-partition topic. What happens?"**
> A rebalance assigns the 3 partitions across the consumers. With 3 partitions and 4 consumers, one
> consumer gets nothing. Parallelism is limited to the partition count. If you remove a consumer,
> Kafka reassigns its partitions to the other consumers.

**6. "At-least-once gives duplicates. How do you keep the warehouse correct?"**
> Make the sink idempotent. Upsert by a business key (your SCD2/MERGE habit), or base the dedup on a
> unique event id. The duplicate overwrites itself and gives the same state. If no dedup is
> possible, use transactions for exactly-once.

# KFK-01 · Interview drills

**1. "Kafka vs. a traditional message queue — one sentence each on the key difference."**
> A queue typically removes a message once it is consumed and acked, so it stores *work to be done*;
> Kafka is a retained, replayable log where consumers track their own offset, so it stores *a history
> that many independent consumers can read and re-read*.

**2. "What does an offset guarantee, and what does it NOT guarantee?"**
> Within one partition, offset order equals append order and is immutable — so per-partition (and
> per-key, since a key maps to one partition) order is guaranteed. It does **not** guarantee any
> ordering across partitions, so there is no global topic ordering.

**3. "A consumer needs 'everything since 09:00'. Walk me through the calls."**
> `offsets_for_times([(topic, partition, 09:00_ms)])` returns the first offset ≥ that timestamp per
> partition; `assign`/`seek` to those offsets; then poll forward to the high watermark. If some
> partition has no record ≥ the time, it returns the end offset (nothing to read there).

**4. "Why can't you always replay from offset 0?"**
> Retention (`retention.ms`/`retention.bytes`) deletes the oldest segments, so the earliest *retained*
> offset (the low watermark) may be > 0. Compaction can also remove superseded records per key. You
> can only replay back to what is still on disk.

**5. "How is 'current state per key' represented natively, and where have you seen that already?"**
> As a fold over the log, materialized by a **compacted** topic that keeps the latest record per key.
> `__consumer_offsets` and Kafka Streams KTable changelogs use it. It's the same idea as rebuilding a
> current dimension by replaying change rows and keeping the last version per business key.

**6. "SOX asks you to prove completeness of a replayed rebuild. What do you check?"**
> That the rebuild consumed every offset in `[low, high)` for every partition with no gaps — the
> consumed count per partition equals `high - low`, and the final folded state matches an independent
> recomputation (the oracle). Gaps or an early stop would mean missing facts.

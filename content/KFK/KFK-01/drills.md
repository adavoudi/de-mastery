# KFK-01 · Interview drills

**1. "Kafka vs. a traditional message queue — one sentence each on the key difference."**
> A queue typically deletes a message after a consumer reads and acks it. So it stores *work to be
> done*. Kafka is a retained, replayable log, and each consumer tracks its own offset. So it stores
> *a history that many independent consumers can read and re-read*.

**2. "What does an offset guarantee, and what does it NOT guarantee?"**
> Within one partition, offset order equals append order and is immutable. So Kafka guarantees order
> per partition. It also guarantees order per key, because a key maps to one partition. It does
> **not** guarantee any order across partitions, so there is no global topic order.

**3. "A consumer needs 'everything since 09:00'. Walk me through the calls."**
> 1. `offsets_for_times([(topic, partition, 09:00_ms)])` returns the first offset ≥ that timestamp
>    for each partition.
> 2. `assign`/`seek` to those offsets.
> 3. Poll forward to the high watermark.
>
> If a partition has no record ≥ the time, the call returns the end offset. There is nothing to read
> in that partition.

**4. "Why can't you always replay from offset 0?"**
> Retention (`retention.ms`/`retention.bytes`) deletes the oldest segments. So the earliest
> *retained* offset (the low watermark) may be > 0. Compaction can also remove superseded records
> for each key. You can replay only back to the data that is still on disk.

**5. "How is 'current state per key' represented natively, and where have you seen that already?"**
> It is a fold over the log. A **compacted** topic materializes it and keeps the latest record per
> key. `__consumer_offsets` and Kafka Streams KTable changelogs use it. It is the same idea as
> rebuilding a current dimension. You replay change rows and keep the last version for each business
> key.

**6. "SOX asks you to prove completeness of a replayed rebuild. What do you check?"**
> Check that the rebuild consumed every offset in `[low, high)` for every partition, with no gaps.
> The consumed count for each partition equals `high - low`. The final folded state matches an
> independent recomputation (the oracle). A gap or an early stop means that facts are missing.

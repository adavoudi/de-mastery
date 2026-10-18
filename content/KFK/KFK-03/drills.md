# KFK-03 · Interview drills

**1. "Three settings to raise producer throughput, and the cost of each?"**
> `linger.ms` up (adds latency, bigger batches), `batch.size` up (more memory, bigger requests),
> `compression.type=lz4/zstd` (CPU for less network/disk). Together: fewer, larger, compressed
> requests — higher throughput, slightly higher latency.

**2. "Does enabling idempotence hurt throughput?"**
> Negligibly. It pins `acks=all` and bounds in-flight to <= 5, but with batching + compression you
> keep high throughput while gaining no-duplicate, no-reorder guarantees. For most pipelines it's a
> free safety upgrade and is on by default in modern clients.

**3. "How does the idempotent producer actually prevent duplicates and reordering?"**
> The producer gets a producer id (PID) and assigns a monotonically increasing sequence number per
> partition. The broker tracks the last sequence per PID+partition and rejects a duplicate
> (seq already seen) or an out-of-order seq, so a retried/resent batch can't duplicate or reorder.

**4. "Why does max.in.flight matter for ordering?"**
> With >1 in-flight request, a failed-then-retried request can be delivered after a later request
> that already succeeded, flipping order within the partition. Idempotence guarantees ordering only
> while in-flight <= 5; above that the guarantee is lost.

**5. "How do you guarantee all events for one account stay in order?"**
> Give every record a key = account id. The default partitioner hashes the key so all of an
> account's events go to one partition, where offset order = append order. Combined with idempotence,
> retries don't disturb that order.

**6. "You must not duplicate financial events. acks=all alone — enough?"**
> No. `acks=all` is about durability, not deduplication; a lost ack still triggers a retry that can
> duplicate. You need `enable.idempotence=true` (which also sets acks=all) so the broker drops the
> duplicate by sequence number. For cross-topic atomicity you escalate to transactions (KFK-05).

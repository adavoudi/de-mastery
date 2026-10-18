# KFK-03 · Interview drills

**1. "Three settings to raise producer throughput, and the cost of each?"**
> - `linger.ms` up: adds latency and makes bigger batches.
> - `batch.size` up: uses more memory and makes bigger requests.
> - `compression.type=lz4/zstd`: uses CPU to save network and disk.
>
> Together they give fewer, larger, compressed requests. The result is higher throughput and
> slightly higher latency.

**2. "Does enabling idempotence hurt throughput?"**
> Only a very small amount. It pins `acks=all` and limits in-flight requests to <= 5. With batching
> and compression you keep high throughput, and you also get no duplicates and no reordering. For
> most pipelines this is a free safety upgrade. Modern clients enable it by default.

**3. "How does the idempotent producer actually prevent duplicates and reordering?"**
> The producer gets a producer id (PID). It assigns a sequence number to each partition, and the
> number always increases. The broker tracks the last sequence for each PID and partition. It
> rejects a duplicate (a sequence it already saw) and an out-of-order sequence. So a retried or
> resent batch cannot duplicate or reorder.

**4. "Why does max.in.flight matter for ordering?"**
> Suppose there is more than 1 in-flight request. A request can fail and be retried. Then it can
> arrive after a later request that already succeeded. This flips the order within the partition.
> Idempotence guarantees ordering only while in-flight is <= 5. Above 5, the guarantee is lost.

**5. "How do you guarantee all events for one account stay in order?"**
> Give every record a key = account id. The default partitioner hashes the key. So all events of an
> account go to one partition, where offset order = append order. With idempotence, retries do not
> change that order.

**6. "You must not duplicate financial events. acks=all alone — enough?"**
> No. `acks=all` is about durability, not deduplication. A lost ack still triggers a retry, and the
> retry can duplicate the record. You need `enable.idempotence=true` (which also sets acks=all).
> Then the broker drops the duplicate by its sequence number. For atomicity across topics, you use
> transactions (KFK-05).

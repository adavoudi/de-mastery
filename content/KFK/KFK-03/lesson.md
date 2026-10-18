# KFK-03 · Producers — batching, idempotence, ordering under retries

## Why the producer is where correctness is won or lost

The producer is deceptively simple: `produce(topic, key, value)`. But between that call and a durable
record on the leader sit the decisions that determine throughput, ordering and whether a retry
creates a duplicate. This module is about tuning for throughput **without** sacrificing the two
guarantees a SOX-style financial feed needs: **no duplicates** and **no reordering per key**, even
when the network hiccups and the client retries.

## How the producer works (objective ccdak:2)

A record you `produce()` is not sent immediately. It is appended to a per-partition **batch** in an
in-memory buffer (`buffer.memory`), and a background I/O thread sends batches to brokers.

- **`linger.ms`** — how long the producer waits to let a batch fill before sending. `0` sends as
  soon as possible (lowest latency, smallest batches); a few milliseconds dramatically improves
  throughput by amortising per-request overhead across more records.
- **`batch.size`** — the max bytes per partition batch. Bigger batches = fewer, larger requests =
  higher throughput, at the cost of latency and memory.
- **`compression.type`** (`lz4`, `zstd`, `snappy`, `gzip`) — compresses each batch. `lz4`/`zstd`
  give large throughput and storage wins for text/JSON at low CPU cost. Compression happens per
  batch, so it pairs with a non-zero `linger.ms`.
- **The partitioner** decides which partition a record goes to. With a **key**, the default
  partitioner hashes the key so the **same key always lands in the same partition** — the basis of
  per-key ordering. With no key it spreads records (sticky batching) across partitions for
  throughput. Ordering is only ever guaranteed *within* a partition.

Tuning for throughput is therefore: raise `linger.ms` a little, raise `batch.size`, turn on `lz4`
compression — send fewer, bigger, compressed batches.

## Retries, ordering, and idempotence (objective ccdak:2)

Here is the trap. A produce request can fail transiently (leader moved, timeout). The producer
**retries**. Two bad things can happen:

1. **Duplicates.** The record was actually written, but the ack was lost; the retry writes it again.
2. **Reordering.** With more than one in-flight request per connection, a retried request can land
   *after* a later request that already succeeded — so within one partition the order flips.

The fix is the **idempotent producer** (`enable.idempotence=true`). The producer gets a producer id
and per-partition sequence numbers; the broker rejects duplicates and out-of-order sequences, so a
retry neither duplicates nor reorders. Enabling it also sets safe defaults: `acks=all`,
`retries=INT_MAX` (bounded by `delivery.timeout.ms`), and `max.in.flight.requests.per.connection<=5`.
That last bound is the subtle one: idempotence preserves ordering across retries **only** up to 5
in-flight requests; set it to `6+` and you can still reorder on retry.

So the throughput-safe recipe is: **`enable.idempotence=true`** (which pins `acks=all` and bounded
in-flight), a tuned `linger.ms`/`batch.size`/`compression.type` for throughput, and a key per record
so each key's events stay ordered in one partition. You get bigger batches AND no dup / no reorder.

> We never grade "how many records per second did you hit" — throughput is machine- and
> load-dependent and would be flaky. The exercise grades the **properties** that make throughput
> safe (the config) and the **invariant** that per-key order in the consumed log equals the order
> you produced, even with retries enabled.

## KIP-848 / KIP-932 note

These are consumer-side (KFK-04): the **new consumer group protocol (KIP-848)** and **share groups
(KIP-932)**. They do not change producer semantics; a producer's idempotence/ordering guarantees are
independent of how consumers are grouped.

## From your world

| SOX / dbt / lakehouse | Producer setting | Note |
|---|---|---|
| "every financial event recorded exactly once" | `enable.idempotence=true` | no duplicate on retry |
| "events for one account in the order they happened" | key = account id + idempotence | per-key order in one partition |
| dbt model micro-batch size vs latency | `linger.ms` / `batch.size` | throughput vs latency trade |
| Parquet/zstd column compression | `compression.type=zstd` | same idea, per batch on the wire |

## When NOT to turn every knob up

- **Strict low latency.** A big `linger.ms` adds latency; if you need sub-10ms end-to-end, keep
  `linger.ms` small and accept smaller batches.
- **You truly don't need ordering or dedup.** For lossy metrics you can leave idempotence off and
  use `acks=1` for speed — but never for financial/SOX data.
- **Huge in-flight for raw speed.** Don't raise `max.in.flight` above 5 with idempotence expecting
  ordering; above 5 the ordering guarantee under retry is lost.

## Further reading

Official docs only:

- Producer configs (`linger.ms`, `batch.size`, `compression.type`, `acks`, `retries`):
  <https://kafka.apache.org/documentation/#producerconfigs>
- Idempotent producer (`enable.idempotence`, `max.in.flight.requests.per.connection`):
  <https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence>
- Message delivery semantics:
  <https://kafka.apache.org/documentation/#semantics>
- Confluent — Optimizing the producer for throughput:
  <https://developer.confluent.io/courses/architecture/producer-hardware/>

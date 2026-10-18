# FND-05 · Delivery semantics

## Why it exists

In a distributed pipeline, messages get lost and messages get duplicated — and you usually cannot
have neither. The reason is the **two generals problem**: when two parties coordinate over an
unreliable channel, no finite exchange of acknowledgements can make both *certain* the other
received the last message. A sender that didn't get an ack has two choices: **resend** (risking a
duplicate) or **give up** (risking a loss). That single dilemma is why "exactly-once" is subtle
and why idempotency matters more than any framework feature.

The three delivery guarantees are the three ways to resolve that dilemma:

- **At-most-once** — never resend. Fast, lossy. Fine for metrics you can lose.
- **At-least-once** — always resend until acked. Never loses, but **duplicates**. The default for
  most systems.
- **Exactly-once** — looks like each event affected the result once. Achieved not by magic but by
  **at-least-once delivery + idempotent or transactional processing** at the sink.

## How it works

**The honest definition of exactly-once.** You almost never get exactly-once *delivery* on the
wire. What you get is **effectively-once processing**: the event may be delivered many times, but
its *effect on the output* happens once. Two mechanisms achieve it:

1. **Idempotency via a dedup key.** Give every event a stable unique id (a natural key, or a
   `(producer, sequence)` pair). The sink remembers which ids it has already applied and ignores
   repeats. `UPSERT ... ON CONFLICT (id) DO NOTHING` and Iceberg/Delta `MERGE INTO ... WHEN MATCHED`
   are this. The defining property: **applying the same event twice equals applying it once**
   (`f(f(x)) = f(x)`).
2. **Transactions / atomic commit.** Bind the write and the offset/position commit into one atomic
   unit, so you never acknowledge consuming an event without also durably writing its effect (and
   vice versa). Kafka's transactional producer and Flink's two-phase-commit sinks do this.

**Choosing the dedup key is the whole game.** It must be:
- **Stable** across retries (the same logical event must produce the same key every time — so a
  random UUID generated at send time is wrong; the id must come from the event's identity).
- **Unique** per logical event (so two genuinely different events never collide and get one
  dropped).

Pick it wrong and you either keep duplicates (key too coarse / unstable) or silently drop distinct
events (key too coarse the other way). Dedup must also be **idempotent and order-independent**:
re-running it, or seeing the duplicates in any order, yields the same set.

## From your world

This is the discipline behind **re-running a dbt model safely**. An incremental model with a
`unique_key` and a `MERGE`/`delete+insert` strategy is idempotent by construction: run it twice on
overlapping data and the target is identical — exactly `f(f(x)) = f(x)`. A model that does a bare
`INSERT` of "new" rows without a key is at-least-once with duplicates: a re-run double-counts.

It is also the backbone of your **SOX completeness and accuracy** controls. *Completeness* =
"no acknowledged transaction is lost" → you need at-least-once (never at-most-once) at ingestion.
*Accuracy* = "nothing is counted twice" → you need idempotent application keyed on the transaction's
natural id. In the commerce feed, the generator deliberately emits **duplicate order-item events**;
the correct `(company_id, order_item_id)`-keyed dedup makes reprocessing safe, while a naive load
inflates revenue — a textbook accuracy-control failure.

## When NOT to use it

- **Don't pay for exactly-once where at-most-once is fine.** For best-effort telemetry or
  sampled metrics, the transactional overhead (extra round trips, held locks, larger state) isn't
  worth it.
- **Don't use a time-based or random key for dedup.** A key that changes on retry (wall-clock,
  fresh UUID) is unstable and defeats dedup entirely.
- **Don't dedup on too coarse a key** (e.g. `order_id` when an order legitimately has many line
  items) — you'll drop distinct events. Match the key to the event's true grain.
- **Don't claim exactly-once with a non-transactional sink.** If the write and the offset commit
  aren't atomic, a crash between them gives you a duplicate or a loss, whatever the producer config
  says.

## Further reading

- Akidau, T. et al. (2015). *The Dataflow Model.* (effectively-once semantics) VLDB.
  https://research.google/pubs/pub43864/
- Apache Kafka Documentation — *Message delivery semantics (at-most / at-least / exactly-once).*
  https://kafka.apache.org/documentation/#semantics
- Confluent — *Exactly-once semantics in Apache Kafka (idempotent & transactional producer).*
  https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
- Apache Flink Documentation — *End-to-end exactly-once and the two-phase-commit sink.*
  https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#fault-tolerance
- Lamport/Gray — *The Two Generals Problem* (background).
  https://en.wikipedia.org/wiki/Two_Generals%27_Problem

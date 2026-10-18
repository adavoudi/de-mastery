# FND-05 · Delivery semantics

## Why it exists

In a distributed pipeline, messages get lost and messages get duplicated. Usually you cannot
avoid both problems. The reason is the **two generals problem**. Two parties coordinate over an
unreliable channel. No finite exchange of acknowledgements can make both parties *certain* that
the other party received the last message.

A sender that did not get an ack has two choices:

- **Resend.** This risks a duplicate.
- **Give up.** This risks a loss.

This one dilemma is why "exactly-once" is subtle. It is also why idempotency matters more than
any framework feature.

The three delivery guarantees are the three ways to resolve that dilemma:

- **At-most-once** — never resend. Fast, but it loses messages. This is fine for metrics that you
  can lose.
- **At-least-once** — always resend until the receiver acks. It never loses messages, but it
  **duplicates** them. This is the default for most systems.
- **Exactly-once** — each event looks as if it affected the result once. Magic does not achieve
  this. **At-least-once delivery + idempotent or transactional processing** at the sink achieves
  it.

## How it works

**The honest definition of exactly-once.** You almost never get exactly-once *delivery* on the
wire. What you get is **effectively-once processing**. The system may deliver the event many
times, but the *effect on the output* happens once. Two mechanisms achieve this:

1. **Idempotency via a dedup key.** Give every event a stable unique id (a natural key, or a
   `(producer, sequence)` pair). The sink remembers which ids it already applied. It ignores
   repeats. `UPSERT ... ON CONFLICT (id) DO NOTHING` and Iceberg/Delta `MERGE INTO ... WHEN
   MATCHED` do this. The defining property is: **applying the same event twice equals applying
   it once** (`f(f(x)) = f(x)`).
2. **Transactions / atomic commit.** Bind the write and the commit of the offset/position into
   one atomic unit. Then you never acknowledge consuming an event without also durably writing
   its effect (and the reverse). Kafka's transactional producer and Flink's two-phase-commit
   sinks do this.

**Choosing the dedup key is the whole game.** The key must be:
- **Stable** across retries. The same logical event must produce the same key every time. So a
  random UUID that you generate at send time is wrong. The id must come from the identity of the
  event.
- **Unique** per logical event. Then two genuinely different events never collide, and the
  system does not drop one of them.

Suppose you pick the key wrong. Then one of two things happens:

- You keep duplicates (the key is too fine or unstable). Two copies of the same event get
  different keys.
- You silently drop distinct events (the key is too coarse). Two different events get the same
  key.

Dedup must also be **idempotent and order-independent**. You can re-run it, or see the duplicates
in any order. The result is the same set.

## From your world

This is the discipline behind **re-running a dbt model safely**. An incremental model has a
`unique_key` and a `MERGE` or `delete+insert` strategy. It is idempotent by construction. Run it
twice on overlapping data and the target is identical. This is exactly `f(f(x)) = f(x)`.

A model may do a bare `INSERT` of "new" rows without a key. This model is at-least-once with
duplicates. A re-run double-counts.

This is also the backbone of your **SOX completeness and accuracy** controls:

- *Completeness* = "no acknowledged transaction is lost". You need at-least-once (never
  at-most-once) at ingestion.
- *Accuracy* = "nothing is counted twice". You need idempotent application, keyed on the natural
  id of the transaction.

In the commerce feed, the generator deliberately emits **duplicate order-item events**. A correct
dedup keyed on `(company_id, order_item_id)` makes reprocessing safe. A naive load inflates
revenue. This is a textbook failure of an accuracy control.

## When NOT to use it

- **Do not pay for exactly-once where at-most-once is fine.** Take best-effort telemetry or
  sampled metrics. The transactional overhead is not worth it. The overhead is extra round trips,
  held locks, and larger state.
- **Do not use a time-based or random key for dedup.** A key that changes on retry (wall-clock,
  fresh UUID) is unstable. It defeats dedup entirely.
- **Do not dedup on a key that is too coarse** (for example `order_id`, when an order legitimately
  has many line items). You will drop distinct events. Match the key to the true grain of the
  event.
- **Do not claim exactly-once with a non-transactional sink.** Suppose the write and the offset
  commit are not atomic. Then a crash between them gives you a duplicate or a loss, whatever the
  producer config says.

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

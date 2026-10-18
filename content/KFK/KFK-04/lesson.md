# KFK-04 · Consumers & groups — manual commits, at-least-once, no loss across crashes

## Why the commit strategy decides correctness

A consumer reads records and does work with them. For example, it writes to a database, calls an
API, or updates a table. After a crash, one question decides correctness: **when do you record
"I have handled the records up to here"?** The recorded position is the **committed offset**.
Suppose you commit too early. Then a crash loses the records that you did not process
(**at-most-once**). Suppose you commit after processing. Then a crash makes you reprocess the last
few records (**at-least-once**). In this module, you kill a consumer in the middle of a stream.
Then you watch what the next consumer picks up.

## How consumer groups work

- **Group.** Consumers that share a `group.id` form a group. Kafka **divides** the partitions of the
  subscribed topics among them. At any time, exactly one consumer in the group reads each partition.
  If you add a consumer, the partitions **rebalance** to spread the load. If you remove a consumer,
  its partitions move to the remaining consumers. This is how you scale out and how you fail over.
- **Committed offset.** For each group and each partition, Kafka stores "the next offset that this
  group should read". It stores this value in the internal `__consumer_offsets` topic. A new or
  resuming consumer in the group starts from the committed offset. If no committed offset exists,
  it uses `auto.offset.reset` (`earliest` or `latest`).
- **The new protocol (KIP-848).** Kafka 4.x introduces the **consumer group protocol**. It moves
  assignment to the group coordinator on the broker. The old protocol is client-driven and has a
  "stop-the-world" rebalance, where all consumers stop. The new protocol replaces it with an
  incremental reconciliation that the broker drives. This gives fewer and shorter pauses. In the
  API, you still `subscribe`, `poll`, and `commit`. The improvement is in how rebalances happen
  underneath.
- **Share groups (KIP-932).** A newer mode that works like a **queue**. Multiple consumers can share
  the records of a single partition, with **per-record acknowledgement** (like SQS). This replaces
  the rule of one partition for each consumer. It is **early access** in this Kafka line. The graded
  exercise does not use it. You must know that it exists. You must also know when to use it: for
  fan-out work queues where strict partition ownership is the wrong model.

![Four partitions split between two consumers in one group; when C2 crashes its partitions move to C1](diagrams/group-rebalance.svg)

## Commit strategies and delivery semantics

- **Auto-commit** (`enable.auto.commit=true`): the client commits the latest polled offsets at every
  `auto.commit.interval.ms`. It commits *even if you did not finish processing those records*. This
  is convenient. But suppose a crash happens after an auto-commit and before the work is finished.
  Then Kafka **loses** those records. For the in-flight batch, this is effectively at-most-once.
- **Manual commit after processing** (`enable.auto.commit=false`, then `commit()` after the work is
  durably done): the committed offset moves only past records that you actually handled. After a
  crash, you reprocess from the last commit. This is **at-least-once**. You lose no records, and
  duplicates are possible.
- **Manual commit before processing**: this is the anti-pattern. You commit, and then you do the
  work. If a crash happens during the work, you lose records. This is at-most-once. Do not do this.

![Poll, process, commit and a crash: committing first loses records 5-8, committing last reprocesses them](diagrams/commit-crash-timeline.svg)

**At-least-once is the correct default choice** for a feed where completeness is critical. You
never drop an event. You handle the rare duplicate downstream with an idempotent write. This is an
upsert keyed by a business id, which is the same as your dbt/SCD2 MERGE habit. If you need
*exactly-once* across a read-process-write, you use transactions (KFK-05).

**Lag** is `log-end-offset − committed-offset` for each partition. It shows how far behind the group
is. Rising lag means that the consumers cannot keep up. This is the number that you alert on
(KFK-10).

## From your world

| SOX / dbt | Consumer behaviour | Note |
|---|---|---|
| Completeness: never drop a transaction | manual commit **after** processing (at-least-once) | a crash reprocesses, never skips |
| Idempotent `MERGE` absorbs reprocessing | dedup the at-least-once duplicate downstream | upsert by business key |
| High-water mark in an incremental model | the **committed offset** | "where did we get to" |
| Scaling a load across workers | more consumers in the group → rebalance | one partition per consumer |

## When NOT to use manual at-least-once commits

- **Lossy, high-rate telemetry** where it is fine to drop a few records: auto-commit is simpler and
  faster.
- **You need exactly-once end to end** (no downstream dedup is possible): use transactions (KFK-05),
  not plain manual commits.
- **Strict ordering across many consumers**: order is per partition. If the events of one key must
  be strictly ordered, make sure that the key stays on one partition and one consumer.

## Further reading

Official docs only:

- Consumer configs (`enable.auto.commit`, `auto.offset.reset`, `group.id`):
  <https://kafka.apache.org/documentation/#consumerconfigs>
- Consumer group / offset management & delivery semantics:
  <https://kafka.apache.org/documentation/#semantics>
- The new consumer group protocol (KIP-848):
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol>
- Share groups (KIP-932):
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka>

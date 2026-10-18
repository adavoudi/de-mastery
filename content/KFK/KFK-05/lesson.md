# KFK-05 · Transactions & exactly-once — read-process-write, read_committed, fencing

## Why transactions exist

At-least-once (KFK-04) never loses data, but it can duplicate. You depend on an idempotent sink to
remove the duplicates. Sometimes you cannot do this, because the "sink" is **another Kafka topic**.
Then you need this whole step to be **all-or-nothing**: read from A, transform, write to B, and
**mark A as consumed**. Suppose the process dies after it writes to B and before it commits the
offset of A. Then at-least-once reprocesses the input, and B gets a duplicate. Kafka
**transactions** make the write to B *and* the offset commit for A happen **atomically**. A consumer
of B that uses `read_committed` then sees each input reflected **exactly once**. This is the SOX
ledger property: the system writes every entry once and only once, and it drops no entry.

## How exactly-once works

Three mechanisms build on each other:

1. **Idempotent producer** (KFK-03) — *producer retries* cause no duplicates and no reordering. This
   is the foundation. Transactions require it, and they set it automatically.
2. **Transactions** — a producer with a stable **`transactional.id`** can group two things into a single
   atomic unit. The first is produce calls to one or more topics. The second is the consumer offset
   commits for the input. The steps are:
   - `init_transactions()` once at startup. It also **fences** any older producer with the same id.
   - `begin_transaction()`.
   - `produce(...)` the outputs.
   - `send_offsets_to_transaction(consumer_positions, consumer_group_metadata)` — the input offsets
     become part of the transaction. So "consumed A up to here" commits atomically with "wrote B".
   - `commit_transaction()`. On error, call `abort_transaction()` and nothing becomes visible.
3. **`read_committed` consumer** (`isolation.level=read_committed`) — a downstream consumer of B
   **sees only records from committed transactions**. It cannot see aborted or in-flight
   transactional writes. (The default `read_uncommitted` sees them, which breaks exactly-once.)

Together they give **exactly-once semantics (EOS)** for a read-process-write loop. Each source
record contributes to the output exactly once, even across crashes and retries.

![One read-process-write transaction: output records and input offsets commit together via the commit marker](diagrams/eos-read-process-write.svg)

## Zombie fencing

The dangerous failure is a **zombie**. A zombie is a producer that everyone thinks is dead, so a
replacement took over. But the zombie is still alive and tries to commit. If both producers wrote,
you would get duplicates. Kafka prevents this with the `transactional.id` and an **epoch**.
`init_transactions()` raises the epoch. The broker **rejects** any producer that uses an older epoch
(`ProducerFenced`). So when a new instance initializes with the same `transactional.id`, the old
zombie is fenced out and its commits fail. For this reason, the `transactional.id` must be **stable
for each logical processor**. It must not be random for each run. A random id defeats fencing. Two
instances would have different ids, and both could commit.

![A zombie committing with epoch 4 is rejected once the new instance bumps the epoch to 5](diagrams/eos-zombie-fencing.svg)

## Ledger invariants the test checks

The test verifies exactly-once with **invariants**, not with timing:

- **No loss:** every source record appears in the destination (as a committed record).
- **No duplicate:** no source record appears twice in the destination.
- **Atomicity under abort:** if a transaction aborts, a `read_committed` consumer sees **none** of
  its writes (all-or-nothing).

These are the streaming form of `sum(debits) == sum(credits)` and "no double-posted entry".

## From your world

| SOX / lakehouse | Kafka EOS | Note |
|---|---|---|
| No double-posted ledger entry | transactional produce + `read_committed` | each input reflected once |
| No dropped entry | offsets committed *inside* the transaction | atomic consume+produce |
| Idempotent `MERGE` as a fallback | at-least-once + idempotent sink | when a true transaction isn't available |
| Stable job identity | stable `transactional.id` | enables zombie fencing |

## When NOT to use transactions

- **You do not need atomicity across topics.** Suppose an idempotent upsert downstream already
  removes duplicates (KFK-04). Then at-least-once is simpler and faster. Transactions add latency
  (commit coordination) and operational complexity.
- **Pure ingestion with a non-Kafka idempotent sink.** Write to a database with an upsert keyed by a
  business id. This gives effectively-once without Kafka transactions.
- **Very high throughput, loss-tolerant streams.** The overhead of the transaction coordinator is
  not worth it for telemetry that you can drop.

## Further reading

Official docs only:

- Transactions / EOS (configs: `transactional.id`, `isolation.level`):
  <https://kafka.apache.org/documentation/#semantics>
- Producer configs (`transactional.id`, `enable.idempotence`):
  <https://kafka.apache.org/documentation/#producerconfigs>
- Consumer `isolation.level`:
  <https://kafka.apache.org/documentation/#consumerconfigs_isolation.level>
- Confluent — Transactions / exactly-once semantics:
  <https://developer.confluent.io/courses/architecture/exactly-once/>

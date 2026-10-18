# FLK-06 · State, checkpoints & exactly-once — surviving a TaskManager crash

## Why checkpoints exist

A stateful stream job holds the thing that makes it valuable *in memory*: running balances, dedup
sets, window accumulators, join buffers. A machine dies — a TaskManager is SIGKILLed, a spot
instance is reclaimed, the kernel OOM-kills a process. If that state vanished, the job would restart
from nothing and either **lose** data (never reprocess what was in flight) or **duplicate** it
(reprocess and emit twice). For a financial ledger both are SOX defects: a dropped entry is a
completeness failure, a double-posted entry is an accuracy failure.

Flink's answer is the **distributed snapshot** (the Chandy–Lamport / asynchronous barrier snapshot
algorithm). Periodically the JobManager injects a **checkpoint barrier** into the sources. The
barrier flows through the dataflow with the records; when an operator has seen the barrier on all of
its inputs it snapshots its state to durable storage and forwards the barrier. When every operator
has acknowledged, the checkpoint is **complete** — a globally consistent cut of "all state as of the
same logical point in the stream". On failure, Flink restores every operator from the last complete
checkpoint and **rewinds the sources to the matching offsets**, so processing resumes exactly where
the consistent snapshot was taken.

## How exactly-once actually holds end to end

Checkpoints give exactly-once *for state*. To get exactly-once *to the sink* you need one more thing:

1. **A replayable source.** The source must be able to rewind to the offset recorded in the
   checkpoint (Kafka offsets, a file split position, a bounded sequence). Then no input is skipped or
   re-read incorrectly on restart.
2. **`enableCheckpointing(interval)` with EXACTLY_ONCE mode** (the default). State is snapshotted
   consistently; keyed state (your ledger balances) is part of the snapshot.
3. **A sink that commits on checkpoint.** Two ways:
   - **Transactional / 2PC sink** (e.g. Kafka transactions, or the `FileSink` with
     `OnCheckpointRollingPolicy`): the sink *pre-commits* on checkpoint and only makes data visible
     when the checkpoint is confirmed complete. Output written since the last complete checkpoint but
     not yet committed is rolled back on restart, so a downstream reader never sees duplicates.
   - **Idempotent sink**: an upsert keyed by a business id, so replays overwrite rather than
     duplicate (effectively-once).

With all three, a mid-run crash is invisible in the final committed output: **every input id appears
exactly once** — no loss, no duplication. That is the invariant this module's exercise asserts.

## Savepoints vs checkpoints

- **Checkpoints** are owned by Flink, taken automatically on an interval, tuned for cheap recovery,
  and (by default) cleaned up when the job stops. They exist for *fault tolerance*.
- **Savepoints** are user-triggered, self-contained, portable snapshots meant for *operations*:
  stop a job and resume it later, upgrade the code, rescale, or migrate state. `cancel-with-savepoint`
  stops the job and hands you a path you can restart from. FLK-12 uses this for zero-data-loss
  upgrades.

Both are built on the same snapshot mechanism; the difference is lifecycle and intent.

## What the test does (fault injection, not timing)

The grader runs your job on a **real local cluster** (JobManager + 2 TaskManagers), waits for the
first checkpoint to complete, then **SIGKILLs a TaskManager** — a genuine crash, mid-run. It restarts
a TaskManager so slots are available, lets the job recover from the last checkpoint and run to
completion, then reads the **committed** sink output and asserts the **ledger invariants**:

- **No loss:** every source id is present in the committed output.
- **No duplication:** no source id appears twice.
- **Balanced total:** the count of committed records equals the number of source records.

These are invariants, never wall-clock timings — the lesson of docs/TESTING.md's fault-tolerance
section.

## From your world

| SOX / ledger | Flink | Note |
|---|---|---|
| No dropped entry (completeness) | replayable source + checkpointed state | rewind on restart |
| No double-posted entry (accuracy) | commit-on-checkpoint / transactional sink | uncommitted output rolled back |
| Period close you can re-run | savepoint → restart | portable, user-owned |
| Reconciliation after an incident | ledger invariants over committed output | count/sum match the source |

## When NOT to lean on this

- **Stateless, loss-tolerant telemetry.** If dropping some records is fine and there is no state,
  checkpointing adds overhead for little benefit — raise the interval or disable it.
- **A truly idempotent downstream.** If the sink already dedups by business key, at-least-once +
  idempotency is simpler than a transactional sink (less commit latency).
- **Huge state with tight latency.** Exactly-once with aligned barriers can add latency under
  backpressure; unaligned checkpoints or a looser guarantee may be the right trade (see FLK-11).

## Further reading

Official Flink 2.x docs only:

- Checkpointing (enable, mode, interval):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/>
- Stateful stream processing & the barrier snapshot algorithm:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/stateful-stream-processing/>
- FileSink (commit on checkpoint):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/filesystem/>
- Savepoints:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/>

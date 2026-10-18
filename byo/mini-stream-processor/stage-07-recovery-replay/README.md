# Stage 7 — Recovery from a checkpoint with a replayable source

Checkpoints are only useful if you can **recover** from them. On failure, Flink
restores every operator's state from the last completed checkpoint and rewinds
each **replayable source** (e.g. a Kafka partition) to the offset recorded in
that checkpoint. Combined, this gives **exactly-once** state: no record is lost
and none is counted twice.

## New API this stage

```python
ReplayableSource(records)                 # .replay_from(offset) -> records[offset:]
run_with_recovery(records, key_fn, barrier_every, crash_after) -> {key: sum}
```

## This stage's task

- `ReplayableSource.replay_from(offset)` returns `records[offset:]`.
- `run_with_recovery`: run the stateful-sum job (stage 6) but **crash
  deterministically** after exactly `crash_after` data records. Track the last
  completed checkpoint. On crash, create a fresh operator, restore its state from
  that checkpoint, and **replay the source from the checkpoint's offset**.
- Return the final `{key: sum}`.

## The exactly-once invariant (how it is graded)

The test injects a crash after **every** possible record count and asserts the
recovered result equals a clean batch fold over all records — for each crash
point and several barrier intervals. If you replay from `0` onto the kept state
you double-count; if you resume from the crash point you lose the records between
the checkpoint and the crash. Only replay-from-checkpoint-offset is exactly-once.

## Constraints

- Deterministic crash schedule (no randomness, no sleeps). Earlier stages still
  pass. Suite < 90 s.

## Further reading

- Flink 2.x — Checkpointing & recovery, exactly-once state:
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/

## Run it

```bash
de start FLK-BYO/07
de test
```

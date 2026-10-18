# Stage 6 — Checkpoint barriers (Chandy-Lamport / Flink ABS)

Flink's fault tolerance rests on **Asynchronous Barrier Snapshotting** (ABS, a
variant of Chandy-Lamport). Special **barrier** markers flow with the data; when
a barrier reaches an operator, the operator snapshots its state. The set of
snapshots taken at the same barrier forms a **consistent cut** of the stream.

## New API this stage

```python
Barrier(id)                                   # the checkpoint marker
inject_barriers(records, every, start_id=1)   # -> list with Barriers inserted
StatefulSumOperator(key_fn)                    # running sum; snapshots on Barrier
Checkpoint(id, operator_states, source_offset)
run_with_checkpoints(records, key_fn, barrier_every) -> (outputs, checkpoints)
```

## This stage's task

- `inject_barriers`: insert a `Barrier(id)` after every `every` data events
  (ids `1, 2, 3, ...`).
- `StatefulSumOperator`: per-key running sum; on each event emit
  `(key, running_sum)`; on a `Barrier`, snapshot a **copy** of state into
  `self.checkpoints[id]` *at the barrier*, then forward the barrier; forward
  watermarks.
- `run_with_checkpoints`: run the operator over the barrier-injected stream and
  record a `Checkpoint(id, {"sum": snapshot}, source_offset)` per barrier, where
  `source_offset` is the number of data records consumed so far.

The snapshot is **consistent**: it equals a fold over exactly the first
`source_offset` records — not more, not fewer (the oracle). This is what makes
recovery (stage 7) exactly-once.

## Constraints

- Snapshot a copy (state keeps mutating after the barrier). Deterministic
  schedule, no randomness, no sleeps. Earlier stages still pass. Suite < 60 s.

## Further reading

- Flink 2.x — Stateful Stream Processing / checkpointing (barrier snapshots):
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/stateful-stream-processing/

## Run it

```bash
de start FLK-BYO/06
de test
```

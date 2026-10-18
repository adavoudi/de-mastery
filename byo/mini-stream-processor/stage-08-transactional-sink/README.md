# Stage 8 (optional) — Transactional (2PC) sink: end-to-end exactly-once

Exactly-once *state* (stage 7) is not enough for exactly-once **output**: the
sink must not publish records that belong to a checkpoint that never completed.
Flink's `TwoPhaseCommitSinkFunction` ties sink transactions to checkpoints —
**pre-commit** on the barrier, **commit** on the checkpoint-complete
notification, and **abort** anything not yet committed on recovery.

> This stage is **optional** for the learner, but it is the capstone of the EOS
> story and is fully graded here.

## New API this stage

```python
TransactionalSink()      # write / pre_commit(cid) / commit(cid) / abort(cid) / abort_open()
run_exactly_once_sink(records, barrier_every, crash_after, commit_lag=0) -> list
```

## This stage's task

- `TransactionalSink`: `write` buffers into the open transaction; `pre_commit(cid)`
  moves it to `prepared[cid]` (durable, invisible); `commit(cid)` appends
  `prepared[cid]` to the visible `committed` list **exactly once**; `abort(cid)`
  and `abort_open()` discard.
- `run_exactly_once_sink`: on each barrier, pre-commit and queue a pending commit;
  a commit **notification** arrives `commit_lag` barriers later. On a clean finish,
  do a **final** pre-commit of the trailing open transaction and flush all pending
  commits.
- On crash: abort the open transaction and every prepared-but-unnotified
  transaction, then replay the source from the last completed checkpoint's offset
  and commit that tail.

## The EOS invariant (how it is graded)

For **every** crash point and commit lag `0`/`1`, the sorted committed output must
equal the batch oracle — every input appears exactly once, with no duplicates
(uncommitted staging is aborted and replayed) and no loss.

## Constraints

- Deterministic crash schedule, idempotent commit, no sleeps. Earlier stages
  still pass. Suite < 90 s.

## Further reading

- Flink 2.x — End-to-end exactly-once / TwoPhaseCommitSinkFunction:
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/

## Run it

```bash
de start FLK-BYO/08
de test
```

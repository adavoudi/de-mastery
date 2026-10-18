# FLK-12 · Deploy & operate — upgrade a stateful job via savepoint without losing state

## Why savepoints exist

A Flink job that has been running for weeks holds state worth more than the code: running totals,
dedup sets, session windows, machine-learned counters. When you deploy a new version — a bug fix, a
new feature, a rescale — you must **not** throw that state away. A **savepoint** is a
self-contained, user-triggered snapshot of all of a job's state, written in a stable, portable
format. You take a savepoint, stop the old job, start the new job **from** the savepoint, and the
new job resumes with every operator's state intact. This is the streaming equivalent of a
zero-downtime migration: the ledger balances carry forward across the deploy.

Checkpoints (FLK-06) are the *automatic*, system-owned snapshots for fault tolerance; a crash
recovers from the latest checkpoint. **Savepoints** are the *manual*, you-own-them snapshots for
**upgrades and migrations** — they are not expired automatically and are the hand-off between job
versions.

## How a stateful upgrade works (objective dse-flink:savepoints)

### 1. Keep state in Flink-managed state, not instance fields

State that survives a savepoint must live in Flink's managed state:

- **Keyed state** (`ValueState`, `ListState`, `MapState`, …) obtained from the runtime context,
  scoped per key — the common case.
- **Operator state** (`ListState` via `CheckpointedFunction`) for non-keyed, per-subtask state like
  a source's read offset.

A plain Java **instance field** is *not* checkpointed. A counter kept in `transient long n` looks
fine while the job runs, but on restore it resets to zero — the classic "my state vanished after the
deploy" bug.

### 2. Assign stable operator UIDs

A savepoint maps each piece of saved state to an operator by its **UID**. If you don't call
`.uid("…")`, Flink derives a UID from the operator's position in the graph, which **changes** when
you edit the job — and then the state can't be mapped back. Always give stateful operators an
explicit, stable `.uid()` so an upgraded job can find its state. (`allowNonRestoredState` lets you
*drop* state deliberately when you remove an operator; it is not a substitute for stable UIDs.)

### 3. The upgrade cycle

```
# stop the old job and snapshot its state
flink stop --savepoint-path s3://…/sp <job-id>        # or cancel-with-savepoint

# start the new jar FROM the savepoint
flink run --fromSavepoint s3://…/sp-… new-job.jar
```

The new job's operators restore their state by UID. A keyed counter that stood at 1,000,000 before
the deploy stands at 1,000,000 after it, and processing continues from exactly where it left off —
no double-counting, no reset.

### 4. Managed vs cloud runs

Locally you drive this with a standing JobManager + TaskManager (the graded path here). In
production the **Flink Kubernetes Operator**, **AWS Managed Service for Apache Flink** or
**Confluent Cloud Flink** perform the stop-with-savepoint / start-from-savepoint for you on every
deploy. Those managed runs are flagged ☁️free / 💲 and are **not** graded — the local savepoint
cycle teaches the same guarantee.

## What the test asserts (deterministic, no wall-clock)

The graded test runs a two-phase cycle on a native local cluster:

1. Start the job; a bounded source emits 5 records; the keyed counter reaches **5**. Take a
   savepoint and stop.
2. Restart the **same** job **from the savepoint** with the source extended to 10 records (so it
   replays its restored offset and emits 5 **more**). If the counter's state was truly restored, it
   continues 6…10 and the final count is **10**. If the state was lost (e.g. kept in an instance
   field), it starts from 0 and only counts the 5 new records, ending at **5**.

So the invariant is **"the restored count equals 10, not 5"** — a ledger-style no-loss check, not a
timer. Readiness is polled via the REST job state and the output file; there is no `sleep()` for
correctness.

## From your world

| Deploy / migration | Flink savepoint upgrade | Note |
|---|---|---|
| Zero-downtime schema migration keeping data | stop-with-savepoint → start-from-savepoint | state carries across versions |
| A dbt run that must not drop accumulated marts | restore keyed state by UID | no reset, no double-count |
| Blue/green deploy | old job snapshot → new job restores it | portable savepoint format |
| Idempotent re-run after a fix | `--fromSavepoint` resume | continues, not restarts |

## When NOT to (or when it needs care)

- **Removing or renaming a stateful operator** without planning for its state — you must either keep
  the UID or pass `allowNonRestoredState` to drop it deliberately.
- **Incompatible state schema changes** (changing a state's type) — may not restore; evolve state
  schema deliberately (Avro/POJO state evolution) or migrate.
- **Rescaling** changes key-group assignment; always go through a savepoint, never a plain restart.
- **Treating savepoints as cheap/automatic** — they are manual and can be large; checkpoints remain
  the mechanism for crash recovery.

## Further reading

Official docs only (Flink 2.2):

- Savepoints: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/>
- Assigning operator UIDs / production readiness: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/production_ready/>
- Working with state (keyed vs operator state): <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/>
- Upgrading applications and Flink versions: <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/upgrading/>

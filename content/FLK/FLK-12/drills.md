# FLK-12 · Interview drills

**1. "Checkpoint vs savepoint — when do you use each?"**
> Checkpoints are automatic and owned by Flink for fault tolerance; a crash recovers from the latest
> one and they're auto-expired. Savepoints are manual, portable snapshots I trigger for planned
> operations — upgrades, rescaling, migrations — and they persist until I delete them. I upgrade a
> job via savepoint, not by relying on a checkpoint.

**2. "Walk me through a zero-downtime upgrade of a stateful job."**
> stop-with-savepoint on the running job to snapshot all operator state, then start the new jar
> `--fromSavepoint <path>`. Each operator restores its state by UID and processing resumes exactly
> where it stopped — no reset, no double-count.

**3. "A colleague's counter resets to zero after every deploy. What's wrong?"**
> They're probably keeping the count in a plain instance field, which isn't part of Flink's managed
> state and isn't snapshotted. Move it to keyed `ValueState` (or operator state) so it's included in
> the savepoint and restored.

**4. "Why assign explicit operator UIDs?"**
> The savepoint maps saved state to operators by UID. Without an explicit `.uid()`, Flink derives
> one from the operator's graph position, which changes when you edit the job — so the state can't
> be matched and is lost (or errors). Stable UIDs make upgrades safe.

**5. "You removed a stateful operator but the savepoint still has its state. How do you deploy?"**
> Start with `allowNonRestoredState` so Flink deliberately drops the orphaned state. Without it,
> unmatched state in the savepoint is treated as an error to prevent accidental data loss.

**6. "Why can't you just restart a stateful job with a higher parallelism?"**
> Keyed state is partitioned into key groups assigned to subtasks. Changing parallelism needs those
> key groups redistributed, which only happens by restoring from a savepoint — a plain restart would
> lose or mis-assign state. Rescale always goes through a savepoint.

**7. "Where does a source keep its read offset so a savepoint resumes correctly?"**
> In operator state: a `ListState` managed through `CheckpointedFunction`, snapshotted in
> `snapshotState` and reloaded in `initializeState` when `isRestored()` is true. Then on restore the
> source replays from the saved offset instead of the beginning.

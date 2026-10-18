# FLK-12 · Interview drills

**1. "Checkpoint vs savepoint — when do you use each?"**
> Checkpoints are automatic and Flink owns them for fault tolerance. After a crash, the job recovers from the latest
> one. Flink expires them automatically. Savepoints are manual, portable snapshots that I trigger for planned
> operations (upgrades, rescaling, migrations). They persist until I delete them. I upgrade a
> job through a savepoint. I do not rely on a checkpoint.

**2. "Walk me through a zero-downtime upgrade of a stateful job."**
> Run stop-with-savepoint on the running job to snapshot all operator state. Then start the new jar
> `--fromSavepoint <path>`. Each operator restores its state by UID. Processing resumes exactly
> where it stopped. There is no reset and no double-count.

**3. "The counter of a colleague resets to zero after every deploy. What is wrong?"**
> They probably keep the count in a plain instance field. It is not part of the managed
> state of Flink and Flink does not snapshot it. Move it to keyed `ValueState` (or operator state). Then the
> savepoint includes it and Flink restores it.

**4. "Why assign explicit operator UIDs?"**
> The savepoint maps saved state to operators by UID. Suppose you have no explicit `.uid()`. Then Flink derives
> a UID from the position of the operator in the graph. This position changes when you edit the job. Then Flink cannot
> match the state, and the state is lost (or you get an error). Stable UIDs make upgrades safe.

**5. "You removed a stateful operator but the savepoint still has its state. How do you deploy?"**
> Start with `allowNonRestoredState`, so Flink deliberately drops the orphaned state. Without it,
> Flink treats unmatched state in the savepoint as an error. This prevents accidental data loss.

**6. "Why can't you just restart a stateful job with a higher parallelism?"**
> Flink partitions keyed state into key groups and assigns them to subtasks. When you change the parallelism, Flink must
> redistribute those key groups. This happens only when you restore from a savepoint. A plain restart would
> lose the state or assign it to the wrong subtasks. Rescale always goes through a savepoint.

**7. "Where does a source keep its read offset so a savepoint resumes correctly?"**
> In operator state. This is a `ListState` that you manage through `CheckpointedFunction`. You snapshot it in
> `snapshotState` and reload it in `initializeState` when `isRestored()` is true. Then, on restore, the
> source replays from the saved offset and not from the beginning.

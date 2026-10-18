# FLK-06 · Interview drills

**1. "Walk me through what happens when a TaskManager dies mid-run."**
> The job fails; Flink restarts the affected tasks (per the restart strategy) and restores every
> operator's state from the last COMPLETED checkpoint, rewinding the sources to the offsets recorded
> in that checkpoint. Processing resumes at that consistent cut, so no input is skipped or
> re-emitted incorrectly.

**2. "What is the checkpoint barrier algorithm?"**
> Asynchronous barrier snapshotting (based on Chandy–Lamport). The JobManager injects a barrier into
> the sources; it flows with the records. An operator aligns the barrier across all its inputs, then
> snapshots its state and forwards the barrier. When every operator acknowledges, the checkpoint is
> complete — a globally consistent snapshot — without a stop-the-world pause.

**3. "Checkpointing is on. Do we have exactly-once to the sink?"**
> Not necessarily. Checkpointing gives exactly-once for STATE. The sink must also either commit on
> checkpoint (transactional/2PC — e.g. Kafka transactions or FileSink with OnCheckpointRollingPolicy)
> or be idempotent (upsert by business key). Otherwise a replay after a crash duplicates output.

**4. "How do you prove exactly-once after injecting a crash — without timing?"**
> Assert ledger invariants over the COMMITTED output: no loss (every source id present), no
> duplication (each id once), and a balanced total (committed count == source count). Wall clock is
> only a generous timeout, never a correctness check.

**5. "Checkpoint vs savepoint?"**
> Same snapshot machinery, different lifecycle. Checkpoints are automatic, interval-based, owned by
> Flink and tuned for cheap recovery. Savepoints are user-triggered, self-contained and portable —
> for stop/upgrade/rescale/migrate. cancel-with-savepoint stops a job and gives you a path to resume.

**6. "Why must the source be replayable?"**
> Because the checkpoint records source offsets. Recovery rewinds to them so restored state and
> replayed input line up exactly. A source that cannot rewind (a plain socket) cannot restore that
> alignment, so exactly-once is impossible regardless of the sink.

# FLK-06 · Interview drills

**1. "Walk me through what happens when a TaskManager dies mid-run."**
> The job fails. Flink restarts the affected tasks (according to the restart strategy). It restores
> the state of every operator from the last COMPLETED checkpoint. It also rewinds the sources to the
> offsets recorded in that checkpoint. Processing resumes at that consistent cut. The job skips no
> input and does not emit any input again by mistake.

**2. "What is the checkpoint barrier algorithm?"**
> It is asynchronous barrier snapshotting (based on Chandy–Lamport). The JobManager injects a barrier
> into the sources. The barrier flows with the records. An operator aligns the barrier across all
> its inputs. Then it snapshots its state and forwards the barrier. When every operator
> acknowledges, the checkpoint is complete. It is a globally consistent snapshot, and it needs no
> stop-the-world pause.

**3. "Checkpointing is on. Do we have exactly-once to the sink?"**
> Not necessarily. Checkpointing gives exactly-once for STATE. The sink must also do one of two
> things. It commits on checkpoint (transactional/2PC — for example Kafka transactions or FileSink
> with OnCheckpointRollingPolicy). Or it is idempotent (upsert by business key). If it does neither,
> a replay after a crash duplicates output.

**4. "How do you prove exactly-once after injecting a crash — without timing?"**
> Assert ledger invariants over the COMMITTED output:
> - No loss: every source id is present.
> - No duplication: each id appears once.
> - Balanced total: committed count == source count.
>
> Wall clock is only a generous timeout. It is never a correctness check.

**5. "Checkpoint vs savepoint?"**
> They use the same snapshot machinery but have a different lifecycle. Checkpoints are automatic and
> interval-based. Flink owns them, and they are tuned for cheap recovery. Savepoints are
> user-triggered, self-contained and portable. They are for stop/upgrade/rescale/migrate.
> cancel-with-savepoint stops a job and gives you a path to resume.

**6. "Why must the source be replayable?"**
> The checkpoint records the source offsets. Recovery rewinds to them, so the restored state and the
> replayed input line up exactly. A source that cannot rewind (a plain socket) cannot restore this
> alignment. Then exactly-once is impossible, regardless of the sink.

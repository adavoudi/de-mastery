# FLK-06 · Explain it back (Feynman)

**Prompt A.** "Explain how Flink survives a TaskManager crash without losing or duplicating data.
Name the three ingredients (replayable source, checkpointed state, commit-on-checkpoint sink). Say
what each one contributes. Walk the checkpoint barrier through a two-operator job."

**Prompt B.** "A reviewer says 'we enabled checkpointing, so we have exactly-once.' Explain why this
is only part of the story, and what the sink must do. Then explain how you would *prove* exactly-once
after a crash without timing anything. That means the ledger invariants."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the barrier snapshot: the JobManager injects a barrier at the sources. Operators
      snapshot when they have the barrier on all inputs. The checkpoint completes when all operators
      acknowledge.
- [ ] States that recovery restores operator state AND rewinds sources to the checkpointed offsets.
- [ ] Names the replayable-source requirement and explains why a non-rewindable source breaks
      exactly-once.
- [ ] Explains that checkpointing alone gives exactly-once for state, not for the sink.
- [ ] Explains a commit-on-checkpoint / 2PC sink: data is visible only when the checkpoint is
      complete, and Flink rolls back uncommitted output on restart (no duplicates).
- [ ] Distinguishes savepoints (user-triggered, portable, for upgrade/rescale) from checkpoints.
- [ ] Proves correctness with invariants (no loss, no duplication, balanced totals), not wall clock.

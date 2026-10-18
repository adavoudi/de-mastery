# FLK-06 · Explain it back (Feynman)

**Prompt A.** "Explain how Flink survives a TaskManager crash without losing or duplicating data.
Name the three ingredients (replayable source, checkpointed state, commit-on-checkpoint sink) and
what each contributes. Walk the checkpoint barrier through a two-operator job."

**Prompt B.** "A reviewer says 'we enabled checkpointing, so we have exactly-once.' Explain why that
is only part of the story, and what the sink must do. Then explain how you would *prove* exactly-once
after a crash without timing anything — i.e. the ledger invariants."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the barrier snapshot: JobManager injects a barrier at sources; operators snapshot when
      they have the barrier on all inputs; checkpoint completes when all acknowledge.
- [ ] States recovery restores operator state AND rewinds sources to the checkpointed offsets.
- [ ] Names the replayable-source requirement and why a non-rewindable source breaks exactly-once.
- [ ] Explains that checkpointing alone gives exactly-once for state, not for the sink.
- [ ] Explains a commit-on-checkpoint / 2PC sink: data visible only when the checkpoint is complete;
      uncommitted output rolled back on restart (no duplicates).
- [ ] Distinguishes savepoints (user-triggered, portable, for upgrade/rescale) from checkpoints.
- [ ] Proves correctness with invariants (no loss, no duplication, balanced totals), not wall clock.

# FLK-12 · Explain it back (Feynman)

**Prompt A.** "You need to deploy a new version of a long-running stateful Flink job without losing
its accumulated state. Walk through the savepoint upgrade cycle. What is the difference between a
checkpoint and a savepoint, and why do you use a savepoint here?"

**Prompt B.** "Two things commonly cause 'my state vanished after the deploy': keeping state in the
wrong place, and not pinning operator identity. Explain both — managed state vs an instance field,
and why an explicit `.uid()` matters for restoring state across a job change."

## Rubric — a strong explanation hits these (5–7):

- [ ] Savepoint = manual, user-owned, portable snapshot for upgrades/migrations/rescale.
- [ ] Checkpoint = automatic, system-owned snapshot for crash recovery (auto-expired).
- [ ] Upgrade cycle = stop-with-savepoint → start new jar `--fromSavepoint`.
- [ ] State must live in Flink-managed state (keyed/operator); a plain instance field is not snapshotted and resets.
- [ ] Explicit `.uid()` lets the savepoint map state to the operator across job edits; auto UIDs change with the graph.
- [ ] Source offset = operator state (ListState via CheckpointedFunction, restored on isRestored()).
- [ ] Rescaling must go through a savepoint; `allowNonRestoredState` deliberately drops removed-operator state.

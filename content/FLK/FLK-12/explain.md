# FLK-12 · Explain it back (Feynman)

**Prompt A.** "You need to deploy a new version of a long-running stateful Flink job. You must not lose
its accumulated state. Walk through the savepoint upgrade cycle. What is the difference between a
checkpoint and a savepoint? Why do you use a savepoint here?"

**Prompt B.** "Two things commonly cause 'my state vanished after the deploy'. One is state in the
wrong place. The other is an operator identity that you did not pin. Explain both: managed state vs an instance field,
and why an explicit `.uid()` matters for the restore of state across a job change."

## Rubric — a strong explanation hits these (5–7):

- [ ] Savepoint = manual, user-owned, portable snapshot for upgrades, migrations and rescale.
- [ ] Checkpoint = automatic, system-owned snapshot for crash recovery (auto-expired).
- [ ] Upgrade cycle = stop-with-savepoint → start new jar `--fromSavepoint`.
- [ ] State must live in Flink-managed state (keyed/operator). A plain instance field is not snapshotted and resets.
- [ ] An explicit `.uid()` lets the savepoint map state to the operator across job edits. Automatic UIDs change with the graph.
- [ ] Source offset = operator state (ListState via CheckpointedFunction, restored on isRestored()).
- [ ] Rescaling must go through a savepoint. `allowNonRestoredState` deliberately drops the state of a removed operator.

# FLK-01 · Explain it back (Feynman)

**Prompt A.** "Explain what happens from the moment you submit a Flink job to the moment it is
RUNNING. Name the roles of the JobManager and the TaskManager. Say what a task slot is. Say who holds
the data and who holds no data."

**Prompt B.** "A linear pipeline source → map → window → sink runs at parallelism 4. Use slot sharing
to explain why it needs 4 slots and not 16. Then explain what happens if the cluster has only 2
slots."

## Rubric — a strong explanation hits these (5–7):

- [ ] JobManager = schedules tasks, coordinates checkpoints, reacts to failures. It processes NO data.
- [ ] TaskManager = worker JVM that runs operator subtasks and HOLDS the state.
- [ ] A task slot = a unit of resource isolation inside a TM. A TM with N slots runs N pipeline slices.
- [ ] Slot sharing: subtasks of different operators of the same job share one slot. So slot demand =
      MAX operator parallelism, not the sum.
- [ ] A job that needs more slots than the cluster has cannot be scheduled and never reaches RUNNING.
- [ ] Operator chaining fuses operators that do not shuffle into one task. keyBy/rebalance breaks the chain.
- [ ] (Bonus) A bounded job ends RUNNING → FINISHED. An unbounded job stays RUNNING until you cancel it.

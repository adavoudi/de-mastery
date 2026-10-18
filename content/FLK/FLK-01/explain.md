# FLK-01 · Explain it back (Feynman)

**Prompt A.** "Walk through what happens from the moment you submit a Flink job to the moment it is
RUNNING. Name the JobManager and TaskManager roles, what a task slot is, and who holds the data and
who holds no data."

**Prompt B.** "A linear pipeline source → map → window → sink runs at parallelism 4. Explain, using
slot sharing, why it needs 4 slots and not 16. Then explain what happens if the cluster only has 2
slots."

## Rubric — a strong explanation hits these (5–7):

- [ ] JobManager = schedules tasks, coordinates checkpoints, reacts to failures; processes NO data.
- [ ] TaskManager = worker JVM that runs operator subtasks and HOLDS the state.
- [ ] A task slot = a unit of resource isolation inside a TM; a TM with N slots runs N pipeline slices.
- [ ] Slot sharing: subtasks of different operators of the same job share one slot, so slot demand =
      MAX operator parallelism, not the sum.
- [ ] A job needing more slots than the cluster has cannot be scheduled and never reaches RUNNING.
- [ ] Operator chaining fuses non-shuffling operators into one task; keyBy/rebalance breaks the chain.
- [ ] (Bonus) A bounded job ends RUNNING → FINISHED; an unbounded one stays RUNNING until cancelled.

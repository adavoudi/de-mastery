# SPK-01 · Explain it back (Feynman)

Pick one prompt. Explain it out loud, or in writing, to a teammate who knows Redshift but has
never run Spark. Record yourself, or write about 200 words.

**Prompt A.** "Walk me through what happens, process by process, from the moment I run
`spark.read.parquet(...).groupBy(...).count().show()` on a cluster. Who plans it, who runs it,
and what dies if the driver dies?"

**Prompt B.** "My analyst wants to run a 30 GB SQL aggregation that Redshift already does in
20 seconds. Convince me whether to use Spark or not. Say what *would* change your answer."

## Rubric — a strong explanation covers these (5–7):

- [ ] Names the **driver** as the single planner/coordinator. Says the app dies if the driver dies.
- [ ] Names **executors** (cores = task slots, memory = execution + cache) as the parallel workers.
- [ ] States that **one task = one partition**, and that peak parallelism = `min(partitions, cores)`.
- [ ] Maps driver↔leader node and executors↔compute nodes/slices. Does not claim more than the analogy supports.
- [ ] Identifies the **cluster manager** as a separate layer that allocates machines.
- [ ] For Prompt B: gives the "under ~100 GB / pure SQL a warehouse wins" rule. Names a
      condition that changes it (data grows larger than one machine, or non-SQL/multi-store logic).
- [ ] (Bonus) Mentions **Spark Connect** or **deploy mode** correctly when relevant.

# SPK-01 · Explain it back (Feynman)

Pick one and explain it out loud (or in writing) as if to a teammate who knows Redshift but has
never run Spark. Record yourself or write ~200 words.

**Prompt A.** "Walk me through what happens, process by process, from the moment I run
`spark.read.parquet(...).groupBy(...).count().show()` on a cluster. Who plans it, who runs it,
and what dies if the driver dies?"

**Prompt B.** "My analyst wants to run a 30 GB SQL aggregation that Redshift already does in
20 seconds. Convince me whether to use Spark or not, and say what *would* change your answer."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names the **driver** as the single planner/coordinator and says the app dies if it dies.
- [ ] Names **executors** (cores = task slots, memory = execution + cache) as the parallel workers.
- [ ] States that **one task = one partition**, and peak parallelism = `min(partitions, cores)`.
- [ ] Maps driver↔leader node and executors↔compute nodes/slices without over-claiming the analogy.
- [ ] Identifies the **cluster manager** as a distinct layer that allocates machines.
- [ ] For Prompt B: gives the "under ~100 GB / pure SQL a warehouse wins" rule and names a
      condition that flips it (data outgrows one box, or non-SQL/multi-store logic).
- [ ] (Bonus) Mentions **Spark Connect** or **deploy mode** correctly when relevant.

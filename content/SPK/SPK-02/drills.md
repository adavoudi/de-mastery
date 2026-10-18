# SPK-02 · Interview drills

**1. "Transformation or action: `groupBy().count()`? What about `count()` on a DataFrame?"**
> `groupBy("k").count()` is a *transformation* — it returns a DataFrame (grouped counts), so it's
> lazy. A bare `df.count()` is an *action* — it returns a number and triggers a job. The word
> "count" means different things depending on whether a group precedes it.

**2. "How do you predict the number of stages in a job?"**
> Count the wide transformations (shuffles) on the path from source to action. For a linear plan,
> stages = shuffles + 1. groupBy, non-broadcast join, distinct, repartition, and orderBy each add
> a shuffle; filter/select/withColumn are narrow and pipeline into the current stage.

**3. "Why is a shuffle expensive?"**
> It materializes intermediate data to disk on the map side, moves it across the network
> partitioned by key, and reads it back on the reduce side. That's disk + network + serialization,
> versus narrow work that stays in memory on one partition. Shuffles are where tuning pays off.

**4. "I run two actions on the same transformed DataFrame and it's slow twice. Why?"**
> Transformations are lazy and not memoized, so each action recomputes the full lineage from the
> source. `cache()`/`persist()` (plus a first materializing action) stores the result so later
> actions reuse it.

**5. "Map a Spark Exchange to something in a Redshift EXPLAIN."**
> An Exchange is a shuffle — a data redistribution across partitions. In Redshift's EXPLAIN that's
> a `DS_DIST_*` or `DS_BCAST_*` step: rows moving across slices because the join/group key doesn't
> match the DISTKEY. Same cost, same reason, same thing to design away.

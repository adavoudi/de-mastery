# SPK-02 · Interview drills

**1. "Transformation or action: `groupBy().count()`? What about `count()` on a DataFrame?"**
> `groupBy("k").count()` is a *transformation*. It returns a DataFrame (grouped counts), so it is
> lazy. A bare `df.count()` is an *action*. It returns a number and triggers a job. The word
> "count" means different things, depending on whether a group comes before it.

**2. "How do you predict the number of stages in a job?"**
> Count the wide transformations (shuffles) on the path from the source to the action. For a
> linear plan, stages = shuffles + 1. Each of these adds a shuffle: groupBy, non-broadcast join,
> distinct, repartition, and orderBy. The transformations filter/select/withColumn are narrow.
> They pipeline into the current stage.

**3. "Why is a shuffle expensive?"**
> On the map side, it writes intermediate data to disk. It moves the data across the network,
> partitioned by key. On the reduce side, it reads the data back. This costs disk, network, and
> serialization. Narrow work stays in memory on one partition. Shuffles are where tuning gives
> the most benefit.

**4. "I run two actions on the same transformed DataFrame and it's slow twice. Why?"**
> Transformations are lazy and Spark does not memoize them. So each action recomputes the full
> lineage from the source. `cache()`/`persist()` (plus a first action that materializes the
> data) stores the result. Later actions then reuse it.

**5. "Map a Spark Exchange to something in a Redshift EXPLAIN."**
> An Exchange is a shuffle. It is a data redistribution across partitions. In the Redshift
> EXPLAIN, it is a `DS_DIST_*` step. (`DS_BCAST_INNER` is a broadcast. Its Spark analogue is `BroadcastExchange`.) Rows move across slices because the
> join/group key does not match the DISTKEY. The cost is the same, the reason is the same, and
> you design it away in the same way.

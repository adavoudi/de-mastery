# Interview drills — FND-02

### 1. "What is data skew and how do you detect and fix it?"
**Model answer.** Skew is an uneven distribution of rows across partitions. It is usually caused
by one hot key.

Detection: look at the distribution of partition sizes. Specifically, look at `max/mean` (or
`max/median`) of row counts or bytes. A ratio well above 1 means a straggler task, and the whole
stage waits for it.

Fixes:

- Salt the hot key. Append a bounded random suffix, aggregate per salted key, then re-aggregate.
- Pick a partition key with higher cardinality.
- Broadcast the small side of a join, so that no redistribution is needed.

A larger partition count does *not* fix a single hot key. `hash(k) % P` is still one value.

### 2. "Hash vs range partitioning — when do you choose each?"
**Model answer.** Hash partitioning (`hash(key) % P`) puts equal keys in the same partition. So
joins and group-bys on that key run locally. It spreads distinct keys evenly, but it ignores the
volume of each key. So it skews on hot keys.

Range partitioning assigns contiguous key ranges. It is best for ordered output and range scans.
But you must sample the data to pick balanced boundaries. If you do not, you bring skew back.

Use hash for equi-joins and aggregations on a high-cardinality key. Use range for sorted outputs
and range predicates.

### 3. "Why is the shuffle the thing we optimize around?"
**Model answer.** A shuffle moves records across the network between every mapper and reducer.
The map side partitions the records and spills them to disk. The reduce side pulls its slice from
every source. A shuffle touches the three slowest subsystems: serialization, disk, and network.
Its connection count grows with `mappers × reducers`.

So performance work is mostly about shuffling less:

- Pick a partition key that makes joins collocated.
- Shuffle once and reuse the result.
- Broadcast small tables.
- Avoid repartitions that you do not need.

### 4. "How does this map to Redshift DISTSTYLE?"
**Model answer.** `DISTSTYLE KEY(x)` is hash partitioning on `x`. Two tables keyed on the same
column join without redistribution (collocated). This is the same as two Spark DataFrames that are
partitioned on the join key.

`DISTSTYLE EVEN` is round-robin. It is balanced, but every join redistributes, because the keys
are not in the same place.

`DISTSTYLE ALL` replicates a small dimension to every node. This is exactly a broadcast join.

The skew risk is the same in both worlds. A DISTKEY with low cardinality (like a 3-value
company_id) overloads a few slices.

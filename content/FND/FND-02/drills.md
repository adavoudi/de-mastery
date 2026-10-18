# Interview drills — FND-02

### 1. "What is data skew and how do you detect and fix it?"
**Model answer.** Skew is an uneven distribution of rows across partitions — usually one hot key.
Detection: look at the per-partition size distribution, specifically `max/mean` (or `max/median`)
of row counts or bytes; a ratio well above 1 means a straggler task that the whole stage waits on.
Fixes: salt the hot key (append a bounded random suffix, aggregate per salted key, then
re-aggregate), pick a higher-cardinality partition key, or broadcast the small side of a join so
no redistribution is needed. Increasing the partition count does *not* fix a single hot key —
`hash(k) % P` is still one value.

### 2. "Hash vs range partitioning — when do you choose each?"
**Model answer.** Hash partitioning (`hash(key) % P`) co-locates equal keys, so joins and group-bys
on that key run locally; it spreads distinct keys evenly but ignores per-key volume, so it skews on
hot keys. Range partitioning assigns contiguous key ranges and is best for ordered output and range
scans, but you must sample the data to pick balanced boundaries or you reintroduce skew. Use hash
for equi-joins/aggregations on a high-cardinality key; use range for sorted outputs and range
predicates.

### 3. "Why is the shuffle the thing we optimize around?"
**Model answer.** A shuffle moves records across the network between every mapper and reducer: the
map side partitions and spills records to disk, the reduce side pulls its slice from every source.
It touches the three slowest subsystems — serialization, disk, and network — and its connection
count grows with `mappers × reducers`. So performance work is mostly about shuffling less: pick a
partition key that makes joins collocated, shuffle once and reuse, broadcast small tables, and
avoid gratuitous repartitions.

### 4. "How does this map to Redshift DISTSTYLE?"
**Model answer.** `DISTSTYLE KEY(x)` is hash partitioning on `x`; two tables keyed on the same
column join without redistribution (collocated), the same as two Spark DataFrames partitioned on
the join key. `DISTSTYLE EVEN` is round-robin — balanced but every join redistributes because keys
aren't co-located. `DISTSTYLE ALL` replicates a small dimension to every node, which is exactly a
broadcast join. The skew risk is the same in both worlds: a low-cardinality DISTKEY (like a 3-value
company_id) overloads a few slices.

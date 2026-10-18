# SPK-07 · Interview drills

**1. "repartition vs coalesce — mechanics and when to use each."**
> `repartition(n)` triggers a full shuffle to produce `n` balanced partitions and can increase or
> decrease the count (wide, adds an Exchange). `coalesce(n)` merges adjacent partitions with no
> shuffle and can only reduce (narrow). Use `coalesce` to cut the file count cheaply after a
> filter; use `repartition` to rebalance skew or grow parallelism.

**2. "How do you hit a target output file size?"**
> Estimate the result's total size, divide by the target (e.g. 128 MB–1 GB) to get a partition
> count, then `repartition`/`coalesce` to that count before writing. The number of output files is
> roughly the in-memory partition count times the number of `partitionBy` folders. Never hard-code
> `coalesce(1)` for large data.

**3. "What's wrong with `partitionBy('customer_id')`?"**
> It is high-cardinality, so you get one directory per customer — potentially millions of tiny
> files and directories, the worst small-file explosion, and crippling metadata/listing cost.
> Partition on low-cardinality, frequently filtered columns (date, company).

**4. "Explain bucketing and when it pays off."**
> `bucketBy(n, key)` hashes rows into `n` buckets at write time (into a managed table). Two tables
> bucketed the same way on the same key can be joined with no read-time shuffle, because matching
> keys are colocated — like a Redshift `DISTKEY`. It costs a write-time shuffle and locks the
> bucket count, so it pays off only when you repeatedly join on that stable key.

**5. "repartition vs partitionBy — aren't they the same?"**
> No. `repartition`/`coalesce` set the in-memory task/partition count during computation.
> `partitionBy` sets the on-disk `col=value/` directory layout of the written output. They interact
> at write time (file count), but one is runtime parallelism and the other is physical storage
> layout.

**6. "You see 10,000 part files from one write. Diagnose."**
> Likely `spark.sql.shuffle.partitions` (default 200) times many `partitionBy` folders, or a
> high-cardinality `partitionBy`, or no `coalesce` after a shuffle. Fix by deriving a sane partition
> count from size and coalescing before write, and reconsider the partition column cardinality.

# SPK-07 · Interview drills

**1. "repartition vs coalesce — mechanics and when to use each."**
> `repartition(n)` triggers a full shuffle to produce `n` balanced partitions. It can increase or
> decrease the count (wide, adds an Exchange). `coalesce(n)` merges adjacent partitions with no
> shuffle. It can only reduce the count (narrow). Use `coalesce` to cut the file count cheaply after a
> filter. Use `repartition` to rebalance skew or to grow parallelism.

**2. "How do you hit a target output file size?"**
> Estimate the total size of the result. Divide it by the target size (for example 128 MB–1 GB) to get a
> partition count. Then use `repartition` or `coalesce` to get that count before you write. The number
> of output files is roughly the in-memory partition count times the number of `partitionBy` folders.
> Never hard-code `coalesce(1)` for large data.

**3. "What's wrong with `partitionBy('customer_id')`?"**
> The column has high cardinality, so you get one directory per customer. This can give millions of
> tiny files and directories. It is the worst small-file explosion, and the metadata and listing cost
> becomes very high. Partition on low-cardinality columns that you filter on often (date, company).

**4. "Explain bucketing and when it pays off."**
> `bucketBy(n, key)` hashes rows into `n` buckets at write time (into a managed table). You can join
> two tables that are bucketed the same way on the same key with no read-time shuffle. This works
> because matching keys are colocated. It is like a Redshift `DISTKEY`. Bucketing costs a write-time
> shuffle and fixes the bucket count. It pays off only when you join on that stable key many times.

**5. "repartition vs partitionBy — aren't they the same?"**
> No. `repartition` and `coalesce` set the in-memory task/partition count during computation.
> `partitionBy` sets the on-disk `col=value/` directory layout of the written output. They interact
> at write time (file count). But one is runtime parallelism and the other is physical storage
> layout.

**6. "You see 10,000 part files from one write. Diagnose."**
> The cause is likely one of these:
> - `spark.sql.shuffle.partitions` (default 200) times many `partitionBy` folders.
> - A high-cardinality `partitionBy`.
> - No `coalesce` after a shuffle.
>
> To fix it, calculate a sane partition count from the size and coalesce before you write. Also review
> the cardinality of the partition column.

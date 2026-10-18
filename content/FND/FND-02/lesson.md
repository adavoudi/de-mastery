# FND-02 · Partitioning & shuffles

## Why it exists

Once data lives on many machines, the single most important decision is **which machine holds
which row**. That mapping — the **partitioning scheme** — determines whether a join or aggregation
can run *locally* (each node works on its own data and nobody talks) or must **shuffle** (every
node sends rows to every other node across the network). Shuffles are the dominant cost in
distributed data processing. Partitioning exists to avoid them.

Every engine you will learn is making the same bet: *co-locate the rows that need to be combined,
and the expensive network step disappears.* Spark's partitioner, Kafka's partition key, Iceberg's
sort/partition spec and Redshift's DISTKEY are four spellings of one idea.

## How it works

**Hash partitioning.** Assign row → `hash(key) % P` for `P` partitions. Rows with the same key
always land in the same partition, so a `GROUP BY key` or a join on `key` is local. Hashing
spreads *distinct* keys evenly, but it does **nothing** about a single hot key: if 40% of rows
have `customer_id = 0` (a common NULL-substitute), 40% land in one partition. That is **skew**.

**Range partitioning.** Sort the key space into `P` contiguous ranges (e.g. by sampling the data
to pick boundaries) and assign row → the range its key falls in. Great for range scans and ordered
output; sensitive to boundary choice. Bad boundaries cause skew just like a hot hash key.

**Why shuffles are expensive.** A shuffle writes every record to a local spill file keyed by its
target partition (the *map* side), then every target partition pulls its slice from every source
(the *reduce* side). Cost scales with the data moved and the number of connections
(`O(mappers × reducers)`). It serializes, hits disk, and crosses the network — the three slowest
things a data system does. Fewer and smaller shuffles is most of performance tuning.

**Detecting skew.** You rarely see skew in the plan; you see it in the **partition size
distribution**. One partition 10× the median means one task runs 10× longer and the whole stage
waits on it (a *straggler*). The practical signal: compute per-partition row counts (or bytes) and
look at `max / mean`. A ratio near 1.0 is balanced; a large ratio means a hot key, and the fix is
*salting* (append a random suffix to the hot key to spread it, then re-aggregate) or a different
key.

## From your world

This is **DISTKEY / DISTSTYLE** by another name. In Redshift:
- `DISTSTYLE KEY` on `customer_id` is hash partitioning on `customer_id`. Join two tables both
  distributed on `customer_id` and Redshift does a *collocated* join — no redistribution.
- `DISTSTYLE EVEN` is round-robin: balanced, but **every** join redistributes, because matching
  keys aren't co-located.
- `DISTSTYLE ALL` broadcasts a small dimension to every node — the same trick as a Spark broadcast
  join (which you'll meet in SPK), avoiding the shuffle for small tables.

In your SOX commerce mart, picking `company_id` as a distribution key would be a classic skew
trap: three companies, wildly different volumes — one slice does most of the work. `customer_id`
or `order_id` spreads far better. The skew-detection exercise here is exactly the check you'd run
before trusting a DISTKEY choice.

## When NOT to use it

- **Don't hash-partition on a low-cardinality or skewed key** (a boolean, a status flag,
  `company_id` with 3 values). You get a handful of giant partitions. Use a high-cardinality key
  or salt the hot values.
- **Don't range-partition without sampling** the real data for boundaries — guessed boundaries
  create skew and defeat the purpose.
- **Don't re-partition "just in case."** Each repartition is a full shuffle. Only shuffle when a
  downstream join/aggregation needs the co-location, and ideally shuffle **once** and reuse.
- **Don't partition a small table at all** — broadcast it instead (DISTSTYLE ALL / broadcast join).

## Further reading

- Dean, J. & Ghemawat, S. (2004). *MapReduce.* (shuffle = the sort/transfer between map and
  reduce) https://research.google/pubs/pub62/
- Apache Spark SQL Performance Tuning — *Coalesce hints, repartition, and skew.*
  https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Amazon Redshift Database Developer Guide — *Choosing a data distribution style.*
  https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html
- Apache Kafka Documentation — *Producer partitioning / the partitioner.*
  https://kafka.apache.org/documentation/#producerconfigs_partitioner.class

# Configure a local Spark session and reason about parallelism

The `spark` session that the tests give you runs in **local mode**. Your job is to read its
runtime configuration and calculate how parallel a job would actually be. This is the most
useful piece of architecture intuition.

Implement three functions in `solution.py`:

### `local_cores(spark) -> int`
Return the number of executor **threads (task slots)** that this local session started with.
This is the `N` in `local[N]`. Read it from the master string of the session at runtime. Do not
hard-code it. Do not use `defaultParallelism`. Setting `spark.default.parallelism` can override
that value, so it does not always equal `N`.

### `shuffle_partitions(spark) -> int`
Return the configured number of shuffle partitions (`spark.sql.shuffle.partitions`) as an **int**.

### `peak_parallelism(num_partitions, cores) -> int`
A stage has `num_partitions` partitions. It runs on a cluster with `cores` total cores. Return
the maximum number of tasks that run **at the same time**. Remember: one task processes one
partition. You cannot run more tasks at once than you have cores.

Constraints:
- Do not hard-code `2` or `4`. Read the real values from the session.
- `peak_parallelism` must be a pure function of its two integer arguments.
- Inputs to `peak_parallelism` are non-negative integers.

# Configure a local Spark session and reason about parallelism

The `spark` session you are given in tests runs in **local mode**. Your job is to read its
runtime configuration and compute how parallel a job would actually be — the single most useful
piece of architecture intuition.

Implement three functions in `solution.py`:

### `local_cores(spark) -> int`
Return the number of executor **threads (task slots)** this local session was started with — the
`N` in `local[N]`. Read it from the session's master string at runtime (do not hard-code it, and
do not use `defaultParallelism`: that value can be overridden by `spark.default.parallelism` and
so does not always equal `N`).

### `shuffle_partitions(spark) -> int`
Return the configured number of shuffle partitions (`spark.sql.shuffle.partitions`) as an **int**.

### `peak_parallelism(num_partitions, cores) -> int`
Given a stage with `num_partitions` partitions running on a cluster with `cores` total cores,
return the maximum number of tasks that run **at the same time**. Remember: one task processes one
partition, and you cannot run more tasks at once than you have cores.

Constraints:
- Do not hard-code `2` or `4`; read the real values from the session.
- `peak_parallelism` must be a pure function of its two integer arguments.
- Inputs to `peak_parallelism` are non-negative integers.

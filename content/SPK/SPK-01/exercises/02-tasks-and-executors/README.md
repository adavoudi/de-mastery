# Count the tasks a scan stage runs, and place them on executors

When you open the Spark UI, each stage shows a list of tasks. It also shows which executor ran
each task. This exercise makes that concrete. The first (scan) stage of a job runs **one task
per partition**. The scheduler spreads those tasks across the executors.

Implement two functions in `solution.py`:

### `task_count(df) -> int`
Return the number of tasks that the first stage of `df` would run. This equals the number of
partitions of the underlying RDD of the DataFrame. Read it from the DataFrame at runtime. Do not
guess.

### `tasks_per_executor(num_tasks, num_executors) -> list[int]`
Return a list of length `num_executors`. It gives how many of the `num_tasks` tasks each
executor runs when the scheduler balances them as evenly as possible. The counts must:
- sum to `num_tasks`,
- differ by at most 1 between any two executors,
- be in non-increasing order (busiest executor first).

Example: `tasks_per_executor(7, 3) == [3, 2, 2]`. Example: `tasks_per_executor(6, 3) == [2, 2, 2]`.

Constraints:
- `num_executors >= 1`, `num_tasks >= 0`.
- `task_count` must reflect the actual partitioning of the DataFrame (including after `repartition`).

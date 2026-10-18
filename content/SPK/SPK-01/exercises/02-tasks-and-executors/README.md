# Count the tasks a scan stage runs, and place them on executors

When you open the Spark UI, each stage shows a list of tasks and which executor ran each one.
This exercise makes that concrete: a stage's first (scan) stage runs **one task per partition**,
and the scheduler spreads those tasks across executors.

Implement two functions in `solution.py`:

### `task_count(df) -> int`
Return the number of tasks the first stage of `df` would run. This equals the number of
partitions of the DataFrame's underlying RDD. Read it at runtime from the DataFrame — do not
guess.

### `tasks_per_executor(num_tasks, num_executors) -> list[int]`
Return a list of length `num_executors` giving how many of `num_tasks` tasks each executor runs
when the scheduler balances them as evenly as possible. The counts must:
- sum to `num_tasks`,
- differ by at most 1 between any two executors,
- be returned in non-increasing order (busiest executor first).

Example: `tasks_per_executor(7, 3) == [3, 2, 2]`. Example: `tasks_per_executor(6, 3) == [2, 2, 2]`.

Constraints:
- `num_executors >= 1`, `num_tasks >= 0`.
- `task_count` must reflect the DataFrame's actual partitioning (including after `repartition`).

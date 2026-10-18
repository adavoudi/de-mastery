"""SPK-01 / 02-tasks-and-executors — reference solution."""


def task_count(df):
    return df.rdd.getNumPartitions()


def tasks_per_executor(num_tasks, num_executors):
    base, extra = divmod(num_tasks, num_executors)
    # `extra` executors get one more than the rest; busiest first.
    return [base + 1] * extra + [base] * (num_executors - extra)

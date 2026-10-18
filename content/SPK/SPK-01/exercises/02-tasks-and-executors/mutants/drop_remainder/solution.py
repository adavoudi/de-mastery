"""Mutant: floor division only, dropping the remainder tasks."""


def task_count(df):
    return df.rdd.getNumPartitions()


def tasks_per_executor(num_tasks, num_executors):
    # WRONG: ignores the remainder, so the counts don't sum to num_tasks
    # (7 tasks over 3 executors -> [2,2,2], losing one task).
    base = num_tasks // num_executors
    return [base] * num_executors

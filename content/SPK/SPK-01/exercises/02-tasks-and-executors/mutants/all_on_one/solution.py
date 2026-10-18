"""Mutant: puts every task on the first executor (no balancing)."""


def task_count(df):
    return df.rdd.getNumPartitions()


def tasks_per_executor(num_tasks, num_executors):
    # WRONG: all load on one executor; the others sit idle. Difference exceeds 1.
    out = [0] * num_executors
    out[0] = num_tasks
    return out

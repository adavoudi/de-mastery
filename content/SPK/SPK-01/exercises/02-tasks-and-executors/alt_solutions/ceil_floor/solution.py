"""SPK-01 / 02-tasks-and-executors — alt: ceil/floor math, count via glom()."""

import math


def task_count(df):
    # Materialize the partition structure empirically: glom() groups each partition's
    # rows into one list, so the number of outer elements is the partition/task count.
    return len(df.rdd.glom().collect())


def tasks_per_executor(num_tasks, num_executors):
    hi = math.ceil(num_tasks / num_executors)
    lo = num_tasks // num_executors
    n_hi = num_tasks - lo * num_executors  # how many executors get the ceil
    return [hi] * n_hi + [lo] * (num_executors - n_hi)

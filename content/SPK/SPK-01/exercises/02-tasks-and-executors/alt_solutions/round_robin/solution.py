"""SPK-01 / 02-tasks-and-executors — alt: round-robin assignment, then sort."""


def task_count(df):
    # Same fact, read straight from the RDD.
    return df.rdd.getNumPartitions()


def tasks_per_executor(num_tasks, num_executors):
    counts = [0] * num_executors
    for t in range(num_tasks):
        counts[t % num_executors] += 1
    counts.sort(reverse=True)
    return counts

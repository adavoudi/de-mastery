"""Mutant: task_count reports shuffle.partitions instead of the DataFrame's partitions."""


def task_count(df):
    # WRONG: uses the configured shuffle width (4) regardless of how df is actually
    # partitioned, so it misreports any df that was repartitioned to another count.
    return int(df.sparkSession.conf.get("spark.sql.shuffle.partitions"))


def tasks_per_executor(num_tasks, num_executors):
    base, extra = divmod(num_tasks, num_executors)
    return [base + 1] * extra + [base] * (num_executors - extra)

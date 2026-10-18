"""SPK-01 / 01-configure-session — reference solution."""


def _parse_local_n(master: str) -> int:
    # master looks like "local", "local[2]" or "local[*]".
    if "[" not in master:
        return 1
    inside = master[master.index("[") + 1 : master.index("]")]
    if inside == "*":
        import os
        return os.cpu_count() or 1
    return int(inside)


def local_cores(spark):
    return _parse_local_n(spark.sparkContext.master)


def shuffle_partitions(spark):
    return int(spark.conf.get("spark.sql.shuffle.partitions"))


def peak_parallelism(num_partitions, cores):
    # One task per partition; cannot exceed the number of cores.
    return min(num_partitions, cores)

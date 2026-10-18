"""SPK-01 / 01-configure-session — alt solution: read spark.master via conf, sorted() min."""


def local_cores(spark):
    # Different source for the same fact: the spark.master config entry.
    master = spark.conf.get("spark.master")
    inside = master.split("[", 1)[1].rstrip("]") if "[" in master else "1"
    if inside == "*":
        import os
        return os.cpu_count() or 1
    return int(inside)


def shuffle_partitions(spark):
    return int(spark.conf.get("spark.sql.shuffle.partitions", "200"))


def peak_parallelism(num_partitions, cores):
    return sorted((int(num_partitions), int(cores)))[0]

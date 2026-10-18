"""SPK-01 / 01-configure-session — alt solution: regex on the master string."""

import re


def local_cores(spark):
    master = spark.sparkContext.master
    m = re.search(r"local\[(\d+|\*)\]", master)
    if not m:
        return 1
    token = m.group(1)
    if token == "*":
        import os
        return os.cpu_count() or 1
    return int(token)


def shuffle_partitions(spark):
    conf = dict(spark.sparkContext.getConf().getAll())
    return int(conf["spark.sql.shuffle.partitions"])


def peak_parallelism(num_partitions, cores):
    if num_partitions < cores:
        return num_partitions
    return cores

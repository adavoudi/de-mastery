"""Mutant: peak_parallelism returns max instead of min."""


def local_cores(spark):
    master = spark.sparkContext.master
    inside = master[master.index("[") + 1 : master.index("]")]
    return int(inside)


def shuffle_partitions(spark):
    return int(spark.conf.get("spark.sql.shuffle.partitions"))


def peak_parallelism(num_partitions, cores):
    # WRONG: you cannot run more tasks at once than you have cores.
    return max(num_partitions, cores)

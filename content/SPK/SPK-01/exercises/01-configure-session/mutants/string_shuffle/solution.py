"""Mutant: shuffle_partitions returns the raw string, not an int."""


def local_cores(spark):
    master = spark.sparkContext.master
    inside = master[master.index("[") + 1 : master.index("]")]
    return int(inside)


def shuffle_partitions(spark):
    # WRONG: conf.get returns a str; the task says return an int.
    return spark.conf.get("spark.sql.shuffle.partitions")


def peak_parallelism(num_partitions, cores):
    return min(num_partitions, cores)

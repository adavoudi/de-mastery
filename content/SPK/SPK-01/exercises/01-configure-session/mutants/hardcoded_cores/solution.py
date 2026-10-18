"""Mutant: reads defaultParallelism instead of the N in local[N]."""


def local_cores(spark):
    # WRONG: defaultParallelism can be overridden by spark.default.parallelism and so is
    # NOT the number of local threads. Here it reports 4 while the session is local[2].
    return spark.sparkContext.defaultParallelism


def shuffle_partitions(spark):
    return int(spark.conf.get("spark.sql.shuffle.partitions"))


def peak_parallelism(num_partitions, cores):
    return min(num_partitions, cores)

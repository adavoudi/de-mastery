"""SPK-01 / 01-configure-session — starter.

Implement the three functions so the tests pass. See README.md.
"""


def local_cores(spark):
    # TODO: return the N in local[N], read from the session's master string.
    raise NotImplementedError


def shuffle_partitions(spark):
    # TODO: return spark.sql.shuffle.partitions as an int.
    raise NotImplementedError


def peak_parallelism(num_partitions, cores):
    # TODO: max tasks running at once = min(num_partitions, cores).
    raise NotImplementedError

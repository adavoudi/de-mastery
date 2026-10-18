"""Tests for SPK-01 / 01-configure-session.

The learner reads `local_cores` (the N in local[N]) from the live session's master
string, reads the shuffle width as an int, and computes peak parallelism. We cross-check
against the master string the fixture reports itself, so the test does not bake in the
lab's exact numbers.
"""


def _master_n(spark) -> int:
    master = spark.sparkContext.master
    inside = master[master.index("[") + 1 : master.index("]")]
    if inside == "*":
        import os
        return os.cpu_count() or 1
    return int(inside)


def test_local_cores_matches_master(submission, spark):
    assert submission.local_cores(spark) == _master_n(spark)


def test_local_cores_not_default_parallelism(submission, spark):
    # defaultParallelism may be overridden (here it is 4 while local[2] has 2 threads).
    # A correct answer reads the master N, so when they differ the learner must track N.
    dp = spark.sparkContext.defaultParallelism
    n = _master_n(spark)
    got = submission.local_cores(spark)
    assert got == n
    if dp != n:
        assert got != dp, "local_cores must be the N in local[N], not defaultParallelism"


def test_shuffle_partitions_is_int_and_matches(submission, spark):
    got = submission.shuffle_partitions(spark)
    assert isinstance(got, int), f"expected int, got {type(got).__name__}"
    assert got == int(spark.conf.get("spark.sql.shuffle.partitions"))


def test_peak_parallelism_examples(submission):
    f = submission.peak_parallelism
    assert f(3, 8) == 3     # partitions bound it
    assert f(100, 8) == 8   # cores bound it
    assert f(4, 4) == 4     # equal
    assert f(1, 64) == 1    # a single partition never parallelizes beyond 1


def test_peak_parallelism_is_min_property(submission):
    f = submission.peak_parallelism
    for p, c in [(0, 0), (0, 5), (5, 0), (7, 7), (2, 9), (9, 2), (1000, 16)]:
        assert f(p, c) == min(p, c), f"peak_parallelism({p},{c}) should be {min(p, c)}"


def test_peak_never_exceeds_cores(submission):
    f = submission.peak_parallelism
    for p in (1, 5, 50, 500):
        for c in (1, 2, 4, 8):
            assert f(p, c) <= c, "cannot run more concurrent tasks than cores"

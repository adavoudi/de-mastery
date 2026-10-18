"""Tests for SPK-01 / 02-tasks-and-executors.

task_count is checked against DataFrames repartitioned to known counts (and the live
RDD partition count), so a function that returns a fixed/config number is caught.
tasks_per_executor is checked for the balancing invariants and against an independent
reference computed here.
"""


def _expected_balance(num_tasks, num_executors):
    base, extra = divmod(num_tasks, num_executors)
    counts = [base + 1] * extra + [base] * (num_executors - extra)
    return sorted(counts, reverse=True)


def test_task_count_follows_repartition(submission, spark):
    df = spark.range(0, 200)
    for n in (3, 5, 7):
        rp = df.repartition(n)
        got = submission.task_count(rp)
        assert got == rp.rdd.getNumPartitions() == n, (
            f"task_count should be {n} after repartition({n}), got {got}")


def test_task_count_after_coalesce(submission, spark):
    df = spark.range(0, 200).repartition(8).coalesce(2)
    got = submission.task_count(df)
    assert got == df.rdd.getNumPartitions() == 2


def test_balance_sums_to_total(submission):
    f = submission.tasks_per_executor
    for t, e in [(0, 1), (7, 3), (6, 3), (10, 4), (1, 5), (100, 7)]:
        out = f(t, e)
        assert len(out) == e, f"expected {e} executors, got {len(out)}"
        assert sum(out) == t, f"counts {out} must sum to {t}"


def test_balance_is_even(submission):
    f = submission.tasks_per_executor
    for t, e in [(7, 3), (10, 4), (5, 2), (100, 7), (0, 3)]:
        out = f(t, e)
        assert max(out) - min(out) <= 1, f"counts {out} differ by more than 1"


def test_balance_non_increasing(submission):
    f = submission.tasks_per_executor
    for t, e in [(7, 3), (10, 4), (23, 5)]:
        out = f(t, e)
        assert out == sorted(out, reverse=True), f"{out} not non-increasing"


def test_balance_matches_reference(submission):
    f = submission.tasks_per_executor
    for t, e in [(7, 3), (6, 3), (10, 4), (1, 5), (23, 5), (100, 7), (0, 4)]:
        assert sorted(f(t, e), reverse=True) == _expected_balance(t, e)

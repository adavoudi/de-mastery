"""Tests for SPK-08 / 01-diagnose-oom.

Representative (synthetic, non-copyrighted) Spark log excerpts with their known
diagnosis. The learner's parser must read each log and return the matching
{location, cause, fix}. Each case distinguishes a different signature so a
too-weak classifier (always one answer) is killed.
"""

import pytest

DRIVER_COLLECT = """
25/01/02 10:12:03 ERROR Utils: Uncaught exception in thread driver-heartbeater
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.Dataset.collectToPython(Dataset.scala:...)
  at org.apache.spark.sql.Dataset.toPandas(Dataset.scala:...)
  at app.report.build(report.py:88)  # df.toPandas() on the full table
"""

EXECUTOR_SKEW = """
25/01/02 11:40:55 ERROR Executor: Exception in task 7.0 in stage 4.0 (TID 207)
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.UnsafeExternalRowSorter.sort(...)
25/01/02 11:40:55 WARN TaskSetManager: task 7 processed 5100 MB; median task 102 MB
  (one partition dominates during the join shuffle)
"""

EXECUTOR_SPILL = """
25/01/02 12:03:11 INFO ExternalSorter: Thread 73 spilling in-memory map of 512.0 MB to disk (1 time so far)
25/01/02 12:03:19 INFO ExternalSorter: Thread 73 spilling in-memory map of 512.0 MB to disk (2 times so far)
25/01/02 12:03:28 INFO ExternalAppendOnlyMap: Spilling in-memory map of 480.0 MB to disk (3 times so far)
"""

CONTAINER_OVERHEAD = """
25/01/02 13:22:40 WARN YarnAllocator: Container killed by YARN for exceeding memory limits.
5.6 GB of 5.5 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
"""

CASES = [
    (DRIVER_COLLECT, {"location": "driver", "cause": "collect_to_driver", "fix": "avoid_collect"}),
    (EXECUTOR_SKEW, {"location": "executor", "cause": "skew", "fix": "salt_or_aqe_skew"}),
    (EXECUTOR_SPILL, {"location": "executor", "cause": "spill", "fix": "more_partitions_or_memory"}),
    (CONTAINER_OVERHEAD, {"location": "executor", "cause": "overhead", "fix": "raise_overhead"}),
]

LOCATIONS = {"driver", "executor"}
CAUSES = {"collect_to_driver", "skew", "spill", "overhead"}
FIXES = {"avoid_collect", "salt_or_aqe_skew", "more_partitions_or_memory", "raise_overhead"}


@pytest.mark.parametrize("log,expected", CASES)
def test_diagnosis(submission, log, expected):
    assert submission.diagnose(log) == expected


def test_vocabulary_valid(submission):
    for log, _ in CASES:
        d = submission.diagnose(log)
        assert d["location"] in LOCATIONS
        assert d["cause"] in CAUSES
        assert d["fix"] in FIXES


def test_distinguishes_all_four(submission):
    # A too-weak classifier that returns one fixed answer cannot satisfy all four.
    got = {tuple(submission.diagnose(log).values()) for log, _ in CASES}
    assert len(got) == 4, f"expected 4 distinct diagnoses, got {got}"

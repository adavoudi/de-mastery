"""Drive the JVM-02 stream aggregation through run_junit."""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "OrderStatsTest.java"


def test_order_stats(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

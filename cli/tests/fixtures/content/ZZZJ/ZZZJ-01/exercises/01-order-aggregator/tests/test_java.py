"""Drive the Java/JUnit self-test through the de_testkit run_junit helper.

The learner's Java (from DE_SUBMISSION_DIR, any layout under it) is compiled
together with the JUnit 5/AssertJ test source next to this file, then run with
the JUnit Platform console launcher. Skips cleanly if the JDK or java-deps jars
are missing.
"""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "OrderAggregatorTest.java"


def test_order_aggregator(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

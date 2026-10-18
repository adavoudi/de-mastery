"""Drive the JVM-01 helper port through the de_testkit run_junit helper.

Compiles the learner's Java (from DE_SUBMISSION_DIR, any layout under it)
together with HelpersTest.java and runs JUnit 5 / AssertJ. Skips cleanly if the
JDK or java-deps jars are missing.
"""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "HelpersTest.java"


def test_helpers(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

"""Drive the JVM-05 GC-log analyzer through run_junit.

The GC log fixtures (artifact/gc-leak.log, artifact/gc-healthy.log) are committed
and shown to the learner; the JUnit test embeds identical lines so grading is
deterministic and self-contained.
"""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "GcLogAnalyzerTest.java"


def test_gc_log_analyzer(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

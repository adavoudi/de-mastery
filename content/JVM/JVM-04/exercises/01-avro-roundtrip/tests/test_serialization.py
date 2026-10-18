"""Drive the JVM-04 Avro round-trip / Flink POJO check through run_junit."""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "SerializationTest.java"


def test_serialization(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

"""Drive the KFK-08 Kafka Streams exercise through run_junit.

The learner's Enricher.java (from DE_SUBMISSION_DIR, package layout under
src/main/java) is compiled with the EnrichTest JUnit 5 source next to this file
and run via the JUnit Platform console launcher. The test uses
TopologyTestDriver, so it runs in-process with NO broker and NO RocksDB. Skips
cleanly if the JDK or java-deps jars are missing.
"""

from pathlib import Path

JUNIT_SRC = Path(__file__).parent / "java" / "EnrichTest.java"


def test_enrich_orders(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])

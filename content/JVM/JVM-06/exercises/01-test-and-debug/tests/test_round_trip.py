"""JVM-06 grading, three layers:

1. JUnit/AssertJ unit test of StackTraces (no broker, no Docker) — always runs.
2. Native-broker round-trip of the learner's KafkaRoundTrip.main (real Kafka JVM
   from the vendored tarball, NO Docker) — the no-Docker graded path.
3. Optional lab layer against the `streaming` lab broker — SKIPS cleanly when the
   lab is down (extra only).

The Kafka layer starts a real broker, so the suite can run ~90 s under the 5x
validation shuffle; this is the one Kafka-heavy JVM module and that is expected.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import uuid
from pathlib import Path

import pytest

from de_testkit.java_toolkit import resolve_toolchain

JUNIT_SRC = Path(__file__).parent / "java" / "StackTracesTest.java"


# -- layer 1: pure JUnit (always runs) --------------------------------------
def test_stacktraces_junit(run_junit, submission_dir):
    run_junit(submission_dir, [JUNIT_SRC])


# -- shared helper: compile + run the learner's KafkaRoundTrip.main ----------
def _compile_submission(submission_dir: Path) -> tuple:
    tc = resolve_toolchain()
    classes = Path(tempfile.mkdtemp(prefix="jvm06-classes-"))
    java_files = [str(p) for p in Path(submission_dir).rglob("*.java")]
    cp = tc.classpath()
    comp = subprocess.run([tc.javac, "-cp", cp, "-d", str(classes), *java_files],
                          capture_output=True, text=True, timeout=120)
    if comp.returncode != 0:
        raise AssertionError("javac failed:\n" + comp.stderr)
    return tc, classes, cp


def _run_round_trip(tc, classes, cp, bootstrap: str, topic: str, values: list) -> dict:
    env = dict(os.environ)
    env["PATH"] = str(Path(tc.java).parent) + os.pathsep + env.get("PATH", "")
    run_cp = cp + os.pathsep + str(classes)
    proc = subprocess.run(
        [tc.java, "-cp", run_cp, "de.mastery.jvm06.KafkaRoundTrip",
         bootstrap, topic, *values],
        capture_output=True, text=True, timeout=90, env=env)
    assert proc.returncode == 0, f"KafkaRoundTrip exited {proc.returncode}:\n{proc.stderr[-1500:]}"
    return _parse_tally(proc.stdout)


def _parse_tally(stdout: str) -> dict:
    out = {}
    seen_header = False
    for line in stdout.splitlines():
        line = line.strip()
        if line == "TALLY":
            seen_header = True
            continue
        if seen_header and "=" in line:
            k, _, v = line.partition("=")
            try:
                out[k.strip()] = int(v.strip())
            except ValueError:
                pass
    return out


# -- layer 2: native broker (no Docker) — the graded Kafka path --------------
def test_round_trip_native_broker(kafka_broker, submission_dir):
    tc, classes, cp = _compile_submission(Path(submission_dir))
    topic = "jvm06-" + uuid.uuid4().hex[:8]
    kafka_broker.create_topic(topic, partitions=3, replication_factor=1)
    values = ["a", "a", "b", "a", "b", "c"]
    tally = _run_round_trip(tc, classes, cp, kafka_broker.bootstrap_servers, topic, values)
    # G6 oracle: an independent tally of the produced values.
    assert tally == {"a": 3, "b": 2, "c": 1}, f"got {tally}"


# -- layer 3: streaming lab (SKIPS if lab down) — extra only -----------------
def test_round_trip_lab(lab_kafka, submission_dir):
    # lab_kafka is the bootstrap string "host:port" (SKIPS already if lab is down).
    bootstrap = lab_kafka
    tc, classes, cp = _compile_submission(Path(submission_dir))
    topic = "jvm06-lab-" + uuid.uuid4().hex[:8]
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": bootstrap})
        fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        for f in fs.values():
            f.result(timeout=20)
    except Exception as e:  # pragma: no cover - lab shape varies
        pytest.skip(f"lab topic create unavailable: {e}")
    values = ["x", "y", "x"]
    tally = _run_round_trip(tc, classes, cp, bootstrap, topic, values)
    assert tally == {"x": 2, "y": 1}, f"got {tally}"

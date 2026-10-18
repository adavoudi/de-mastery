"""Phase 3 platform proof: the native-Kafka + Java/JUnit self-test exercises
pass all validity gates, and the no-Docker Avro compatibility checker works.

These validate the FIXTURE exercises under cli/tests/fixtures/content via the
real `validate_exercise` gate runner (same code `de validate` uses), so they
prove G1-G5 for both a Python+Kafka exercise and a Java/JUnit exercise.

Kafka/Java tests SKIP cleanly (never fail) when the native tarball, the vendored
JDK, or the java-deps jars are missing — so CI without those stays green while a
properly provisioned machine (this one) exercises them for real.
"""

from pathlib import Path

import pytest

from de_cli.content import find_exercise
from de_cli.validate import validate_exercise

FIXTURE_CONTENT = Path(__file__).parent / "fixtures" / "content"


# ---------------------------------------------------------------------------
# layer-0: no-process Avro compatibility (always runs)
# ---------------------------------------------------------------------------
V1 = {"type": "record", "name": "Order",
      "fields": [{"name": "id", "type": "long"},
                 {"name": "amt", "type": "double"}]}
V2_OPT = {"type": "record", "name": "Order",
          "fields": [{"name": "id", "type": "long"},
                     {"name": "amt", "type": "double"},
                     {"name": "cur", "type": "string", "default": "USD"}]}
V_REQ = {"type": "record", "name": "Order",
         "fields": [{"name": "id", "type": "long"},
                    {"name": "amt", "type": "double"},
                    {"name": "region", "type": "string"}]}


def test_avro_backward_add_optional():
    from de_testkit.avro_compat import check_compatibility
    # adding an optional field (with default) is BACKWARD compatible
    assert check_compatibility(V2_OPT, V1, "BACKWARD") is True


def test_avro_backward_add_required_breaks():
    from de_testkit.avro_compat import check_compatibility
    # adding a required field with no default breaks BACKWARD compat
    assert check_compatibility(V_REQ, V1, "BACKWARD") is False


def test_avro_forward_and_full():
    from de_testkit.avro_compat import check_compatibility
    assert check_compatibility(V2_OPT, V1, "FORWARD") is True
    assert check_compatibility(V2_OPT, V1, "FULL") is True
    assert check_compatibility(V_REQ, V1, "FULL") is False


# ---------------------------------------------------------------------------
# layer-1: the native-Kafka self-test exercise passes all gates
# ---------------------------------------------------------------------------
def test_kafka_selftest_all_gates():
    from de_testkit import kafka_support as ks
    ok, reason = ks.kafka_available()
    if not ok:
        pytest.skip(reason)
    ex = find_exercise("ZZZK-01/01-roundtrip", FIXTURE_CONTENT)
    assert ex is not None
    result = validate_exercise(ex, runs=3)
    failed = [(g.gate, g.detail) for g in result.gates if not g.passed]
    assert result.ok, f"kafka self-test gates failed: {failed}"
    assert {"G1", "G2", "G3", "G4", "G5"} == {g.gate for g in result.gates}


# ---------------------------------------------------------------------------
# layer-2: the Java/JUnit self-test exercise passes all gates
# ---------------------------------------------------------------------------
def test_java_selftest_all_gates():
    from de_testkit import java_toolkit
    ok, reason = java_toolkit.java_available()
    if not ok:
        pytest.skip(reason)
    ex = find_exercise("ZZZJ-01/01-order-aggregator", FIXTURE_CONTENT)
    assert ex is not None
    result = validate_exercise(ex, runs=3)
    failed = [(g.gate, g.detail) for g in result.gates if not g.passed]
    assert result.ok, f"java self-test gates failed: {failed}"


def test_java_nested_entry_resolves():
    """The CLI must handle a package-path entry like
    src/main/java/de/selftest/OrderAggregator.java."""
    ex = find_exercise("ZZZJ-01/01-order-aggregator", FIXTURE_CONTENT)
    assert ex is not None
    assert ex.entry == "src/main/java/de/selftest/OrderAggregator.java"


# ---------------------------------------------------------------------------
# fetch-script / vendored-artifact presence (informational; skip if absent)
# ---------------------------------------------------------------------------
def test_fetch_scripts_exist():
    labs = Path(__file__).parents[2] / "labs"
    for name in ("fetch_kafka.sh", "fetch_java_deps.sh", "fetch_gradle.sh",
                 "fetch_schema_registry.sh", "smoke_streaming.sh"):
        assert (labs / name).exists(), f"missing labs/{name}"

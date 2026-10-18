"""Stage 1 — ApiVersions over TCP.

Black-box: the broker must answer a real ApiVersions request so that (a) our
raw-socket decoder parses a sane api table and (b) the official Kafka CLI
``kafka-broker-api-versions.sh`` connects and prints the table.
"""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from _broker_harness import (api_versions_request, parse_api_versions_response)

REPO = Path(__file__).resolve().parents[4]


def _kafka_home():
    base = REPO / "labs" / ".kafka"
    if base.is_dir():
        for child in sorted(base.iterdir()):
            if (child / "bin" / "kafka-broker-api-versions.sh").exists():
                return child
    return None


def _jdk_home():
    base = REPO / "labs" / ".jdk"
    if base.is_dir():
        for child in sorted(base.iterdir()):
            if (child / "bin" / "java").exists():
                return child
    return None


def test_api_versions_raw_v3(broker):
    """ApiVersions v3 (flexible). Error code 0 and a non-empty api table that
    includes ApiVersions(18) itself."""
    resp = broker.request(api_versions_request(corr_id=7, api_version=3))
    parsed = parse_api_versions_response(resp, flexible=True)
    assert parsed["error"] == 0
    apis = parsed["apis"]
    assert 18 in apis, "must advertise ApiVersions(18)"
    lo, hi = apis[18]
    assert lo <= 3 <= hi, f"ApiVersions range {lo}..{hi} must cover v3"
    # a usable broker also needs Metadata, Produce and Fetch later; advertise them
    for required in (0, 1, 3):   # Produce, Fetch, Metadata
        assert required in apis, f"api key {required} must be advertised"


def test_correlation_id_echoed(broker):
    resp = broker.request(api_versions_request(corr_id=4242, api_version=3))
    # header is 4 bytes: the correlation id, big-endian
    import struct
    (corr,) = struct.unpack_from(">i", resp, 0)
    assert corr == 4242


def test_official_cli_accepts(broker):
    kafka = _kafka_home()
    jdk = _jdk_home()
    if kafka is None or jdk is None:
        pytest.skip("vendored Kafka/JDK missing: run labs/fetch_kafka.sh + labs/setup_jdk.sh")
    env = dict(os.environ, JAVA_HOME=str(jdk), KAFKA_HEAP_OPTS="-Xmx256m")
    r = subprocess.run(
        [str(kafka / "bin" / "kafka-broker-api-versions.sh"),
         "--bootstrap-server", broker.bootstrap],
        env=env, capture_output=True, text=True, timeout=90)
    assert r.returncode == 0, f"CLI failed:\n{r.stdout}\n{r.stderr}"
    # the tool prints one line per supported api; Produce/Fetch/Metadata appear
    assert "Produce(0)" in r.stdout
    assert "ApiVersions(18)" in r.stdout

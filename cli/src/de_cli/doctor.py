"""Environment checks for ``de doctor``."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass

from de_cli.java_support import resolve as resolve_java


@dataclass
class Check:
    name: str
    ok: bool
    detail: str
    fix: str = ""


def _run(cmd: list[str]) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return p.returncode, (p.stdout + p.stderr).strip()
    except Exception as exc:
        return 1, str(exc)


def check_python() -> Check:
    v = sys.version_info
    ok = (v.major, v.minor) == (3, 12)
    return Check(
        "python", ok, f"Python {v.major}.{v.minor}.{v.micro}",
        "" if ok else "Install Python 3.12 (the project targets ==3.12.*).")


def check_uv() -> Check:
    path = shutil.which("uv")
    if not path:
        return Check("uv", False, "not found",
                     "Install uv: https://docs.astral.sh/uv/getting-started/")
    rc, out = _run(["uv", "--version"])
    return Check("uv", rc == 0, out or "found")


def check_java() -> Check:
    info = resolve_java()
    detail = (f"source={info['source']} major={info['major']} "
              f"home={info['java_home']}")
    if not info["usable"]:
        fix = ("Run `bash labs/setup_jdk.sh` to install a vendored Temurin JDK 21 "
               "into labs/.jdk (no sudo). The CLI and tests will use it automatically.")
    elif not info.get("recommended", True):
        fix = (f"Java {info['major']} runs Spark but is not an LTS/recommended "
               "runtime. For a fully supported JVM, run `bash labs/setup_jdk.sh` "
               "(installs JDK 21 into labs/.jdk, no sudo).")
    else:
        fix = ""
    # Usable is enough to pass the check; the fix note nudges toward an LTS.
    return Check("java (for Spark)", info["usable"], detail, fix)


def check_docker() -> Check:
    path = shutil.which("docker")
    if not path:
        return Check("docker", False, "not found",
                     "Install Docker + Compose v2 for lab profiles (e.g. `core`).")
    rc, out = _run(["docker", "--version"])
    first = out.splitlines()[0] if out else ""
    # compose v2?
    rc2, _ = _run(["docker", "compose", "version"])
    compose = "compose v2 ok" if rc2 == 0 else "compose v2 MISSING"
    ok = rc == 0 and rc2 == 0
    return Check("docker", ok, f"{first}; {compose}",
                 "" if ok else "Install Docker Compose v2 (`docker compose`).")


def check_jars() -> Check:
    """Report whether the pinned Iceberg/Delta Spark runtime JARs are present.

    These are needed by the ``spark_iceberg`` / ``delta_spark`` fixtures (LKH and
    Spark streaming exercises). They are fetched, sha256-verified, into the
    gitignored ``labs/.jars/`` by ``labs/fetch_jars.sh``.
    """
    from de_testkit import jars as jars_mod

    missing = jars_mod.missing(jars_mod.ALL_JARS)
    d = jars_mod.jars_dir()
    if not missing:
        return Check("spark jars (iceberg/delta)", True,
                     f"all {len(jars_mod.ALL_JARS)} present in {d}", "")
    return Check(
        "spark jars (iceberg/delta)", False,
        f"missing {len(missing)}/{len(jars_mod.ALL_JARS)}: {missing}",
        "Run `bash labs/fetch_jars.sh` to download the pinned, sha256-verified "
        "Iceberg + Delta runtime JARs into labs/.jars/ (no Docker needed). "
        "Iceberg/Delta exercise tests SKIP cleanly until these are present.")


def check_kafka() -> Check:
    """Report whether the native Apache Kafka tarball is vendored (KFK track)."""
    from de_testkit import kafka_support as ks

    kh = ks.kafka_home()
    if kh is not None:
        return Check("kafka (native tarball)", True, f"present at {kh}", "")
    return Check(
        "kafka (native tarball)", False, "not found",
        "Run `bash labs/fetch_kafka.sh` to download the pinned, sha512-verified "
        "Kafka 4.x tarball into labs/.kafka/. Kafka exercise tests SKIP cleanly "
        "until it is present.")


def check_java_deps() -> Check:
    """Report whether the JVM-track / Kafka-Streams test jars are present."""
    from de_testkit import java_toolkit

    miss = java_toolkit.missing_jars()
    n = len(java_toolkit.REQUIRED_JARS)
    if not miss:
        return Check("java deps (jvm/streams jars)", True,
                     f"all {n} present in {java_toolkit.java_jars_dir()}", "")
    return Check(
        "java deps (jvm/streams jars)", False, f"missing {len(miss)}/{n}: {miss}",
        "Run `bash labs/fetch_java_deps.sh` to download the pinned, sha256-"
        "verified JUnit/AssertJ/Kafka/Avro jars into labs/.jars/java/. JVM and "
        "KFK-08 exercise tests SKIP cleanly until present.")


def check_gradle() -> Check:
    """Report whether the vendored Gradle distribution is present (JVM-03 only)."""
    from de_testkit import gradle_support

    gh = gradle_support.gradle_home()
    if gh is not None:
        return Check("gradle (JVM-03 only)", True, f"present at {gh}", "")
    return Check(
        "gradle (JVM-03 only)", True, "not vendored (optional)",
        "Only JVM-03 (Gradle Kotlin DSL / shaded jar) needs it. Run "
        "`bash labs/fetch_gradle.sh` to vendor it into labs/.gradle-dist/.")


def check_schema_registry() -> Check:
    """Report whether a native Schema Registry is vendored (optional; KFK-06
    has a no-Docker fastavro fallback)."""
    from de_testkit import avro_compat

    sh = avro_compat.schema_registry_home()
    if sh is not None:
        return Check("schema registry (native, optional)", True,
                     f"present at {sh}", "")
    return Check(
        "schema registry (native, optional)", True, "not vendored (optional)",
        "KFK-06 grades with the no-Docker fastavro compatibility checker "
        "(de_testkit.check_compatibility). For a live registry, run "
        "`bash labs/fetch_schema_registry.sh` or use the `streaming` lab.")


def check_flink() -> Check:
    """Report whether the native Flink dist + extra jars are vendored (FLK)."""
    from de_testkit import flink_support as fs

    home = fs.flink_home()
    if home is None:
        return Check(
            "flink (native dist)", False, "not found",
            "Run `bash labs/fetch_flink.sh` to download the pinned, checksum-"
            "verified Flink 2.x dist + connector/Iceberg jars (and build the "
            "PyFlink venv) into labs/.flink/ + labs/.jars/flink/. FLK exercise "
            "tests SKIP cleanly until present.")
    jd = fs.flink_jars_dir()
    missing = [n for n in fs.EXTRA_JARS if not (jd / n).exists()]
    pyflink = "pyflink venv ok" if fs.pyflink_venv() else "pyflink venv absent (FLK-08 Python SKIPs)"
    if missing:
        return Check(
            "flink (native dist)", False,
            f"dist at {home}; missing jars {missing}",
            "Run `bash labs/fetch_flink.sh` to fetch the connector/Iceberg jars.")
    return Check("flink (native dist)", True,
                 f"dist at {home}; all {len(fs.EXTRA_JARS)} jars; {pyflink}", "")


def check_streamhouse() -> Check:
    """Report whether the native streamhouse artifacts are vendored (STH)."""
    from de_testkit import streamhouse_support as sh

    fh = sh.fluss_home()
    if fh is None:
        return Check(
            "streamhouse (native)", False, "not found",
            "Run `bash labs/fetch_streamhouse.sh` to vendor the pinned, checksum-"
            "verified Fluss 1.0.0 server + Paimon/Fluss Flink jars into "
            "labs/.fluss/ + labs/.jars/streamhouse/ (reuses the Flink dist + "
            "Iceberg jars from fetch_flink.sh). STH exercise tests SKIP cleanly "
            "until present.")
    jd = sh.streamhouse_jars_dir()
    jars = (sh.PAIMON_JAR, sh.FLUSS_CONNECTOR, sh.FLUSS_TIERING)
    missing = [n for n in jars if not (jd / n).exists()]
    if missing:
        return Check(
            "streamhouse (native)", False,
            f"Fluss at {fh}; missing jars {missing}",
            "Run `bash labs/fetch_streamhouse.sh` to fetch the Paimon/Fluss jars.")
    pypaimon = ("pypaimon venv ok" if sh.pypaimon_venv()
                else "pypaimon venv absent (STH-07 Paimon serving SKIPs)")
    ice = ("iceberg runtime ok" if sh.iceberg_available()[0]
           else "iceberg runtime absent (run fetch_flink.sh)")
    return Check("streamhouse (native)", True,
                 f"Fluss at {fh}; all {len(jars)} jars; {pypaimon}; {ice}", "")


def check_ram() -> Check:
    gb = None
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        gb = pages * page_size / (1024 ** 3)
    except (ValueError, OSError):
        pass
    if gb is None:
        return Check("ram", True, "could not determine", "")
    ok = gb >= 15  # labs assume ~16+ GB usable; 32 is the design target
    return Check("ram", ok, f"{gb:.1f} GiB total",
                 "" if ok else "Labs assume >= 16 GB. Run one profile at a time.")


def run_all() -> list[Check]:
    return [
        check_python(),
        check_uv(),
        check_java(),
        check_jars(),
        check_kafka(),
        check_java_deps(),
        check_gradle(),
        check_schema_registry(),
        check_flink(),
        check_streamhouse(),
        check_docker(),
        check_ram(),
    ]

"""Locate the deterministic Spark runtime JARs for Iceberg and Delta.

Tests must never resolve ``--packages`` at runtime (needs network + a writable
ivy cache and is non-deterministic). Instead ``labs/fetch_jars.sh`` downloads the
pinned, sha256-verified JARs into a gitignored ``labs/.jars/`` directory, and the
``spark_iceberg`` fixture / Delta helper put them on the classpath via
``spark.jars``.

This module just finds those files and reports which are present. The exact
version pins live in ``labs/fetch_jars.sh`` and ``labs/versions.lock``.
"""

from __future__ import annotations

from pathlib import Path

from de_testkit.java_env import repo_root

# Expected JAR filenames (version-pinned). Grouped by feature so a fixture can
# ask for just the Iceberg set, just the Delta set, or both.
ICEBERG_JARS = (
    "iceberg-spark-runtime-4.0_2.13-1.10.0.jar",
    "iceberg-aws-bundle-1.10.0.jar",
)
DELTA_JARS = (
    "delta-spark_2.13-4.0.0.jar",
    "delta-storage-4.0.0.jar",
)
ALL_JARS = ICEBERG_JARS + DELTA_JARS


def jars_dir(root: Path | None = None) -> Path:
    return (root or repo_root()) / "labs" / ".jars"


def present(names: tuple[str, ...], root: Path | None = None) -> list[str]:
    """Return absolute paths for the given JAR names that exist on disk."""
    d = jars_dir(root)
    return [str(d / n) for n in names if (d / n).exists()]


def missing(names: tuple[str, ...], root: Path | None = None) -> list[str]:
    d = jars_dir(root)
    return [n for n in names if not (d / n).exists()]


def jars_classpath(names: tuple[str, ...], root: Path | None = None) -> str | None:
    """Comma-joined ``spark.jars`` value, or ``None`` if any JAR is missing."""
    if missing(names, root):
        return None
    return ",".join(present(names, root))

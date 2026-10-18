"""Locate a usable Java runtime for Spark.

Resolution order:
1. ``labs/.jdk/`` (a vendored Temurin JDK 21 installed by ``labs/setup_jdk.sh``) —
   preferred, because the system Java may be too new for Spark.
2. ``JAVA_HOME`` if it points at a real ``bin/java``.
3. ``java`` on ``PATH``.

Spark 4.x requires Java 17 or newer. The officially *recommended* runtimes are the
LTS releases 17/21 (and 25). Feature releases in between (e.g. 24) are not an
official target, but Spark 4.2 does run on them for the operations this platform
uses. So the policy is:

* Java major < 17  -> unusable (Spark will refuse to start).
* Java major >= 17 -> usable; if it is not an LTS/recommended release we mark it
  ``recommended=False`` and ``de doctor`` suggests installing JDK 21 via
  ``labs/setup_jdk.sh`` for a fully supported runtime.
"""

from __future__ import annotations

import os
import re
import subprocess
from functools import lru_cache
from pathlib import Path

# Minimum major Spark 4.x will start on.
MIN_MAJOR = 17
# Officially recommended / tested runtimes.
RECOMMENDED_MAJORS = {17, 21, 25}


def repo_root(start: Path | None = None) -> Path:
    """Walk up from ``start`` (or cwd) to the first dir containing pyproject.toml."""
    cur = (start or Path.cwd()).resolve()
    for p in [cur, *cur.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return cur


def vendored_jdk(root: Path | None = None) -> Path | None:
    """Return ``labs/.jdk/<dir>`` containing ``bin/java`` if present."""
    root = root or repo_root()
    jdk_base = root / "labs" / ".jdk"
    if not jdk_base.is_dir():
        return None
    # Direct layout: labs/.jdk/bin/java
    if (jdk_base / "bin" / "java").exists():
        return jdk_base
    # Nested layout: labs/.jdk/jdk-21.../bin/java
    for child in sorted(jdk_base.iterdir()):
        if (child / "bin" / "java").exists():
            return child
    return None


def _java_major(java_home: Path) -> int | None:
    java_bin = java_home / "bin" / "java"
    if not java_bin.exists():
        return None
    try:
        out = subprocess.run(
            [str(java_bin), "-version"],
            capture_output=True, text=True, timeout=15,
        ).stderr
    except Exception:
        return None
    m = re.search(r'version "(\d+)', out)
    if not m:
        return None
    return int(m.group(1))


@lru_cache(maxsize=1)
def resolve() -> dict:
    """Return a dict describing the chosen Java.

    Keys: ``java_home`` (str|None), ``major`` (int|None), ``usable`` (bool),
    ``recommended`` (bool), ``source`` (str), ``reason`` (str).
    """
    root = repo_root()

    def describe(source: str, home: Path) -> dict:
        major = _java_major(home)
        usable = bool(major and major >= MIN_MAJOR)
        recommended = major in RECOMMENDED_MAJORS
        if not major:
            reason = "could not determine Java version"
        elif not usable:
            reason = (f"Java {major} is too old for Spark 4.x (needs >= {MIN_MAJOR}). "
                      f"Run labs/setup_jdk.sh to install JDK 21.")
        elif not recommended:
            reason = (f"Java {major} works but is not an LTS/recommended runtime. "
                      f"For a fully supported JVM run labs/setup_jdk.sh (JDK 21).")
        else:
            reason = f"Java {major} is a recommended runtime"
        return {"java_home": str(home), "major": major, "usable": usable,
                "recommended": recommended, "source": source, "reason": reason}

    # 1. Vendored JDK wins if present.
    vend = vendored_jdk(root)
    if vend is not None:
        return describe("labs/.jdk", vend)

    candidates: list[tuple[str, Path]] = []
    jh = os.environ.get("JAVA_HOME")
    if jh and (Path(jh) / "bin" / "java").exists():
        candidates.append(("JAVA_HOME", Path(jh)))

    from shutil import which
    jbin = which("java")
    if jbin:
        home = Path(jbin).resolve().parent.parent
        candidates.append(("PATH", home))

    # Prefer a recommended candidate, then any usable one, else report the first.
    described = [describe(src, home) for src, home in candidates]
    for d in described:
        if d["usable"] and d["recommended"]:
            return d
    for d in described:
        if d["usable"]:
            return d
    if described:
        return described[0]
    return {
        "java_home": None, "major": None, "usable": False, "recommended": False,
        "source": "none",
        "reason": "No Java found. Run labs/setup_jdk.sh to install JDK 21.",
    }


def apply_to_env(env: dict | None = None) -> dict:
    """Return a copy of ``env`` (default ``os.environ``) with JAVA_HOME/PATH set to
    the resolved usable JDK, if one exists. Leaves env unchanged otherwise."""
    env = dict(os.environ if env is None else env)
    info = resolve()
    if info["usable"] and info["java_home"]:
        jh = info["java_home"]
        env["JAVA_HOME"] = jh
        env["PATH"] = str(Path(jh) / "bin") + os.pathsep + env.get("PATH", "")
    return env

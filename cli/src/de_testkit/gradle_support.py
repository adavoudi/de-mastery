"""Vendored Gradle support for JVM-03 (Gradle Kotlin DSL build + shaded jar).

Only JVM-03 needs a real Gradle build; every other Java exercise uses the fast
``run_junit`` javac path. This module locates the pinned Gradle distribution
(``labs/.gradle-dist`` from ``labs/fetch_gradle.sh``) and runs it with a
repo-local, gitignored Gradle user home (``labs/.gradle-home``) so builds are
reproducible and don't touch ``~/.gradle``.

Budget: a warm offline build is ~15-30 s. Keep JVM-03's single graded build
``timeout_s`` generous (e.g. 240) but run exactly one build per candidate.
``run_gradle`` SKIPS cleanly when Gradle or Java is missing.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from de_testkit.java_env import repo_root, resolve as resolve_java


def gradle_home(root: Path | None = None) -> Path | None:
    """``labs/.gradle-dist/gradle-<ver>`` if it has bin/gradle."""
    base = (root or repo_root()) / "labs" / ".gradle-dist"
    if not base.is_dir():
        return None
    for child in sorted(base.iterdir()):
        if (child / "bin" / "gradle").exists():
            return child
    return None


def gradle_user_home(root: Path | None = None) -> Path:
    d = (root or repo_root()) / "labs" / ".gradle-home"
    d.mkdir(parents=True, exist_ok=True)
    return d


def gradle_available(root: Path | None = None) -> tuple[bool, str]:
    if gradle_home(root) is None:
        return False, ("Gradle not vendored. Run `bash labs/fetch_gradle.sh` "
                       "(only JVM-03 needs it).")
    info = resolve_java()
    if not info["usable"]:
        return False, f"Java unusable: {info['reason']}"
    return True, ""


@dataclass
class GradleResult:
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def run_gradle(project_dir: str | Path, tasks: list[str], *,
               offline: bool = True, extra_args: list[str] | None = None,
               timeout_s: int = 240, assert_success: bool = True,
               root: Path | None = None) -> GradleResult:
    """Run ``gradle <tasks>`` in ``project_dir`` with the vendored distribution
    and the repo-local Gradle user home. SKIPS cleanly if Gradle/Java missing."""
    import pytest

    ok, reason = gradle_available(root)
    if not ok:
        pytest.skip(reason)

    gh = gradle_home(root)
    info = resolve_java()
    env = dict(os.environ)
    env["JAVA_HOME"] = info["java_home"]
    env["PATH"] = str(Path(info["java_home"]) / "bin") + os.pathsep + env.get("PATH", "")
    env["GRADLE_USER_HOME"] = str(gradle_user_home(root))
    # keep the daemon off so no JVM lingers between candidates
    env["GRADLE_OPTS"] = "-Dorg.gradle.daemon=false -Xmx512m"

    cmd = [str(gh / "bin" / "gradle"), *tasks, "--no-daemon", "--console=plain"]
    if offline:
        cmd.append("--offline")
    if extra_args:
        cmd += extra_args
    proc = subprocess.run(cmd, cwd=str(project_dir), env=env,
                          capture_output=True, text=True, timeout=timeout_s)
    res = GradleResult(returncode=proc.returncode, stdout=proc.stdout,
                       stderr=proc.stderr)
    if assert_success and not res.ok:
        pytest.fail(f"gradle {tasks} failed (rc={proc.returncode}):\n"
                    f"{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    return res

"""JVM-03: build a runnable fat jar with the vendored Gradle (offline) and assert
its production-grade properties: Main-Class manifest, dependency classes bundled
(fat), no duplicate class entries, no leftover signature files, and that it
actually runs on the vendored JDK 21.

Skips cleanly if Gradle or Java is not vendored (run labs/fetch_gradle.sh /
labs/setup_jdk.sh). This is the one JVM module whose suite may take ~30 s: a
single Gradle build is run per candidate.
"""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

from de_testkit.gradle_support import run_gradle
from de_testkit.java_env import repo_root, resolve as resolve_java


DEPS_DIR = repo_root() / "labs" / ".jars" / "java"
MAIN_CLASS = "de.mastery.jvm03.Job"
AVRO_CLASS_ENTRY = "org/apache/avro/Schema.class"
JOB_CLASS_ENTRY = "de/mastery/jvm03/Job.class"


def _gradle_args() -> list[str]:
    return [f"-PdepsDir={DEPS_DIR}", f"-PmainClass={MAIN_CLASS}"]


def _find_fat_jar(submission_dir: Path) -> Path | None:
    libs = Path(submission_dir) / "build" / "libs"
    if not libs.is_dir():
        return None
    cands = sorted(libs.glob("*-all.jar"))
    return cands[0] if cands else None


def _build(submission_dir) -> Path:
    if not DEPS_DIR.exists():
        pytest.skip("java-deps jars missing; run `bash labs/fetch_java_deps.sh`.")
    key = str(Path(submission_dir).resolve())
    cached = _BUILD_CACHE.get(key)
    if cached is not None:
        if isinstance(cached, Exception):
            raise cached
        return cached
    try:
        # One build per candidate (budget ~15-30 s), reused by every assertion below.
        r = run_gradle(submission_dir, ["clean", "fatJar"], offline=True,
                       extra_args=_gradle_args(), timeout_s=240,
                       assert_success=False)
        if not r.ok:
            raise AssertionError(
                "gradle clean fatJar failed (does a `fatJar` task exist and build?):\n"
                + r.stdout[-1500:] + "\n" + r.stderr[-1500:])
        jar = _find_fat_jar(submission_dir)
        assert jar is not None and jar.exists(), (
            "expected a runnable fat jar at build/libs/<project>-all.jar")
        _BUILD_CACHE[key] = jar
        return jar
    except BaseException as e:  # cache the failure so we don't rebuild 4x
        if isinstance(e, Exception):
            _BUILD_CACHE[key] = e
        raise


_BUILD_CACHE: dict = {}


def test_fat_jar_bundles_dependency(submission_dir):
    jar = _build(submission_dir)
    with zipfile.ZipFile(jar) as z:
        names = z.namelist()
    assert JOB_CLASS_ENTRY in names, "the job's own class must be in the jar"
    assert AVRO_CLASS_ENTRY in names, (
        "Avro classes must be bundled (this is a FAT jar) — the thin default jar fails here")


def test_manifest_has_main_class(submission_dir):
    jar = _build(submission_dir)
    with zipfile.ZipFile(jar) as z:
        manifest = z.read("META-INF/MANIFEST.MF").decode("utf-8")
    assert f"Main-Class: {MAIN_CLASS}" in manifest, "manifest must declare the Main-Class"


def test_no_signature_files_and_no_duplicate_classes(submission_dir):
    jar = _build(submission_dir)
    with zipfile.ZipFile(jar) as z:
        names = z.namelist()
    # No leftover signed-jar signatures (would fail the JVM verifier).
    sigs = [n for n in names if n.upper().endswith((".SF", ".DSA", ".RSA"))]
    assert not sigs, f"signature files must be excluded, found: {sigs[:5]}"
    # Exactly one copy of each class entry (a correct, de-duplicated fat jar).
    classes = [n for n in names if n.endswith(".class")]
    dupes = {n for n in classes if classes.count(n) > 1}
    assert not dupes, f"duplicate class entries present: {list(dupes)[:5]}"


def test_jar_runs_on_vendored_jdk(submission_dir):
    jar = _build(submission_dir)
    info = resolve_java()
    if not info["usable"]:
        pytest.skip(info["reason"])
    java = str(Path(info["java_home"]) / "bin" / "java")
    proc = subprocess.run([java, "-jar", str(jar)],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, (
        f"`java -jar` failed (rc={proc.returncode}):\n{proc.stdout}\n{proc.stderr}")
    assert "jvm03 job ok" in proc.stdout, f"unexpected output: {proc.stdout!r}"

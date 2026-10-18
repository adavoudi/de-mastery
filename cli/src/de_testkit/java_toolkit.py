"""Java/JUnit exercise support for the JVM track and KFK-08 (Kafka Streams).

Keeps Java exercises pytest-driven: a Python test calls ``run_junit(...)`` which
  1. javac-compiles the learner's ``.java`` files (from ``DE_SUBMISSION_DIR``)
     together with the exercise's JUnit 5 / AssertJ test sources, against the
     fixed classpath of sha256-pinned jars in ``labs/.jars/java``;
  2. runs the JUnit Platform console launcher on the compiled tests;
  3. parses the launcher output into a structured result and (by default)
     asserts success, surfacing each failed JUnit test readably in pytest.

Everything uses the vendored JDK 21 (``labs/.jdk``). If the JDK or the jars are
missing the helper raises ``pytest.skip`` so the suite skips cleanly (never
fails) with a message pointing at the fetch scripts.

The learner's Java entry may be a single file (``OrderAggregator.java``) or a
package layout (``src/main/java/com/acme/OrderAggregator.java``). ``run_junit``
compiles *all* ``.java`` under the submission dir, so either layout works.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from de_testkit.java_env import repo_root, resolve as resolve_java


def java_jars_dir(root: Path | None = None) -> Path:
    return (root or repo_root()) / "labs" / ".jars" / "java"


# The jars fetched by labs/fetch_java_deps.sh. kafka-streams-test-utils gives the
# TopologyTestDriver; avro/jackson are for JVM-04 serialization.
REQUIRED_JARS = (
    "junit-platform-console-standalone-1.11.4.jar",
    "assertj-core-3.27.3.jar",
    "kafka-clients-4.3.1.jar",
    "kafka-streams-4.3.1.jar",
    "kafka-streams-test-utils-4.3.1.jar",
    "avro-1.12.0.jar",
    "jackson-databind-2.18.2.jar",
    "jackson-core-2.18.2.jar",
    "jackson-annotations-2.18.2.jar",
    "slf4j-api-2.0.16.jar",
    "slf4j-simple-2.0.16.jar",
)


def missing_jars(root: Path | None = None) -> list[str]:
    d = java_jars_dir(root)
    return [n for n in REQUIRED_JARS if not (d / n).exists()]


def java_available(root: Path | None = None) -> tuple[bool, str]:
    info = resolve_java()
    if not info["usable"]:
        return False, (f"Java unusable: {info['reason']} "
                       "Run `bash labs/setup_jdk.sh`.")
    miss = missing_jars(root)
    if miss:
        return False, ("Java dependency jars missing "
                       f"({len(miss)}/{len(REQUIRED_JARS)}). Run "
                       "`bash labs/fetch_java_deps.sh`.")
    return True, ""


@dataclass
class JavaToolchain:
    java_home: Path
    jars_dir: Path

    @property
    def javac(self) -> str:
        return str(self.java_home / "bin" / "javac")

    @property
    def java(self) -> str:
        return str(self.java_home / "bin" / "java")

    def classpath(self, extra: list[str] | None = None) -> str:
        jars = [str(self.jars_dir / n) for n in REQUIRED_JARS]
        if extra:
            jars += [str(e) for e in extra]
        return os.pathsep.join(jars)


def resolve_toolchain(root: Path | None = None):
    import pytest

    ok, reason = java_available(root)
    if not ok:
        pytest.skip(reason)
    info = resolve_java()
    return JavaToolchain(java_home=Path(info["java_home"]),
                         jars_dir=java_jars_dir(root))


@dataclass
class JUnitResult:
    returncode: int
    tests_found: int
    tests_failed: int
    stdout: str
    stderr: str
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and self.tests_failed == 0 and self.tests_found > 0


_SUMMARY_FOUND = re.compile(r"\[\s*(\d+)\s+tests\s+found\s*\]")
_SUMMARY_FAILED = re.compile(r"\[\s*(\d+)\s+tests\s+failed\s*\]")


def _collect_java(files_root: Path) -> list[str]:
    return [str(p) for p in files_root.rglob("*.java")]


def run_junit(submission_dir: str | Path, test_sources: list[str | Path], *,
              extra_classpath: list[str] | None = None,
              assert_success: bool = True,
              select_classes: list[str] | None = None,
              timeout_s: int = 120,
              root: Path | None = None) -> JUnitResult:
    """Compile the learner's Java (from ``submission_dir``) + the given JUnit
    ``test_sources`` and run them with the JUnit Platform console launcher.

    * ``test_sources`` — paths to the exercise's ``*Test.java`` files (and any
      helper sources). They are copied next to the learner code so cross-package
      compilation just works.
    * ``extra_classpath`` — additional jars (rarely needed; the standard set is
      always on the classpath).
    * ``select_classes`` — restrict execution to these fully-qualified test
      classes; default is ``--scan-classpath`` which runs everything found.
    * ``assert_success`` — if True (default), fail the pytest test with the
      JUnit failure details when any Java test fails.

    Returns a ``JUnitResult``. Skips cleanly if the JDK/jars are missing.
    """
    import pytest

    tc = resolve_toolchain(root)
    submission_dir = Path(submission_dir)

    work = Path(tempfile.mkdtemp(prefix="junit-"))
    try:
        src = work / "src"
        src.mkdir()
        # copy learner java (preserve any package dirs) + test sources
        for jf in _collect_java(submission_dir):
            rel = Path(jf).relative_to(submission_dir)
            dest = src / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(jf, dest)
        for ts in test_sources:
            ts = Path(ts)
            shutil.copy2(ts, src / ts.name)

        classes = work / "classes"
        classes.mkdir()
        cp = tc.classpath(extra_classpath)

        all_java = _collect_java(src)
        if not all_java:
            raise RuntimeError(f"no .java files found under {submission_dir}")

        compile_cmd = [tc.javac, "-cp", cp, "-d", str(classes), *all_java]
        comp = subprocess.run(compile_cmd, capture_output=True, text=True,
                              timeout=timeout_s)
        if comp.returncode != 0:
            res = JUnitResult(returncode=comp.returncode, tests_found=0,
                              tests_failed=0, stdout=comp.stdout,
                              stderr=comp.stderr,
                              failures=["COMPILE ERROR:\n" + comp.stderr])
            if assert_success:
                pytest.fail("Java compilation failed:\n" + comp.stderr)
            return res

        run_cp = cp + os.pathsep + str(classes)
        launch = [tc.java, "-jar",
                  str(tc.jars_dir / "junit-platform-console-standalone-1.11.4.jar"),
                  "execute", "-cp", run_cp, "--details=tree",
                  "--disable-ansi-colors", "--fail-if-no-tests"]
        if select_classes:
            for c in select_classes:
                launch += ["-c", c]
        else:
            launch += ["--scan-classpath", str(classes)]
        env = dict(os.environ)
        env["CLASSPATH"] = run_cp
        run = subprocess.run(launch, capture_output=True, text=True,
                             timeout=timeout_s, env=env)

        out = run.stdout + "\n" + run.stderr
        m_found = _SUMMARY_FOUND.search(out)
        m_failed = _SUMMARY_FAILED.search(out)
        tests_found = int(m_found.group(1)) if m_found else 0
        tests_failed = int(m_failed.group(1)) if m_failed else (
            0 if run.returncode == 0 else 1)
        failures = _parse_failures(out)

        res = JUnitResult(returncode=run.returncode, tests_found=tests_found,
                          tests_failed=tests_failed, stdout=run.stdout,
                          stderr=run.stderr, failures=failures)
        if assert_success and not res.ok:
            detail = "\n".join(failures) if failures else out[-2000:]
            pytest.fail(
                f"JUnit: {tests_failed} failed / {tests_found} found "
                f"(rc={run.returncode}).\n{detail}")
        return res
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _parse_failures(out: str) -> list[str]:
    """Extract the 'Failures (N):' block from the console launcher output."""
    lines = out.splitlines()
    failures: list[str] = []
    capture = False
    for ln in lines:
        if re.match(r"Failures\s*\(\d+\):", ln):
            capture = True
            continue
        if capture:
            if ln.strip() == "" and failures and not ln.startswith(" "):
                break
            failures.append(ln)
    return [f for f in failures if f.strip()]

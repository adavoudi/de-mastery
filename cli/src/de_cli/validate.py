"""Validity gates G1-G5 for exercises (docs/TESTING.md).

For a given exercise we run its ``tests/`` suite repeatedly, each time pointing
``DE_SUBMISSION_DIR`` at a different candidate directory:

* G1 starter fails >= 1 test
* G2 solution passes all
* G3 every alt_solutions/* passes all (>= 2 required for code exercises)
* G4 every mutants/* fails >= 1 test
* G5 solution passes `runs` times with pytest-randomly shuffling (no flakes)

``validate_exercise`` returns a structured result; the CLI renders it as a table and
sets the process exit code.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from de_cli.content import Exercise
from de_cli.java_support import spark_env


@dataclass
class GateResult:
    gate: str
    name: str
    passed: bool
    detail: str = ""


@dataclass
class ExerciseValidation:
    exercise_id: str
    gates: list[GateResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(g.passed for g in self.gates)


def _entry_files(ex: Exercise) -> list[str]:
    entry = ex.entry
    return entry if isinstance(entry, list) else [entry]


def _run_pytest(tests_dir: Path, submission_dir: Path, entry: str,
                timeout_s: int, shuffle_seed: int | None = None) -> int:
    """Run the exercise tests against one submission dir. Returns pytest exit code."""
    env = spark_env(os.environ)
    env["DE_SUBMISSION_DIR"] = str(submission_dir)
    env["DE_SUBMISSION_ENTRY"] = entry
    args = [
        sys.executable, "-m", "pytest", str(tests_dir),
        "-q", "-p", "no:cacheprovider",
        f"--timeout={timeout_s}",
    ]
    if shuffle_seed is None:
        args += ["-p", "no:randomly"]
    else:
        args += [f"--randomly-seed={shuffle_seed}"]
    proc = subprocess.run(args, env=env, capture_output=True, text=True,
                          timeout=timeout_s + 60)
    return proc.returncode


def _candidate_dirs(ex: Exercise, kind: str) -> list[tuple[str, Path]]:
    base = ex.dir
    if kind in ("starter", "solution"):
        d = base / kind
        return [(kind, d)] if d.is_dir() else []
    # alt_solutions/* or mutants/*
    parent = base / kind
    if not parent.is_dir():
        return []
    return [(f"{kind}/{p.name}", p) for p in sorted(parent.iterdir()) if p.is_dir()]


def validate_exercise(ex: Exercise, runs: int = 5) -> ExerciseValidation:
    result = ExerciseValidation(exercise_id=ex.id)
    tests_dir = ex.dir / "tests"
    entry = _entry_files(ex)[0]
    timeout_s = ex.timeout_s

    if not tests_dir.is_dir():
        result.gates.append(GateResult("G0", "tests/ exists", False,
                                        f"no tests dir at {tests_dir}"))
        return result

    # G1: starter fails >= 1 test (nonzero exit)
    starter = _candidate_dirs(ex, "starter")
    if not starter:
        result.gates.append(GateResult("G1", "starter fails", False, "no starter/"))
    else:
        rc = _run_pytest(tests_dir, starter[0][1], entry, timeout_s)
        result.gates.append(GateResult(
            "G1", "starter fails", rc != 0,
            "starter failed (good)" if rc != 0 else "starter PASSED (should fail)"))

    # G2: solution passes all
    solution = _candidate_dirs(ex, "solution")
    if not solution:
        result.gates.append(GateResult("G2", "solution passes", False, "no solution/"))
    else:
        rc = _run_pytest(tests_dir, solution[0][1], entry, timeout_s)
        result.gates.append(GateResult(
            "G2", "solution passes", rc == 0,
            "solution passed" if rc == 0 else "solution FAILED (rc!=0)"))

    # G3: every alt passes all; >= 2 required
    alts = _candidate_dirs(ex, "alt_solutions")
    if len(alts) < 2:
        result.gates.append(GateResult(
            "G3", "alts pass (>=2)", False, f"found {len(alts)} alt(s), need >= 2"))
    else:
        failures = []
        for name, d in alts:
            rc = _run_pytest(tests_dir, d, entry, timeout_s)
            if rc != 0:
                failures.append(name)
        result.gates.append(GateResult(
            "G3", "alts pass (>=2)", not failures,
            "all alts passed" if not failures else f"failed: {failures}"))

    # G4: every mutant fails >= 1 test
    mutants = _candidate_dirs(ex, "mutants")
    if len(mutants) < 3:
        result.gates.append(GateResult(
            "G4", "mutants killed (>=3)", False,
            f"found {len(mutants)} mutant(s), need >= 3"))
    else:
        survivors = []
        for name, d in mutants:
            rc = _run_pytest(tests_dir, d, entry, timeout_s)
            if rc == 0:
                survivors.append(name)  # a mutant that PASSED = survived = bad
        result.gates.append(GateResult(
            "G4", "mutants killed (>=3)", not survivors,
            "all mutants killed" if not survivors else f"survived: {survivors}"))

    # G5: solution passes `runs` times with shuffling, no flakes
    if solution:
        flaky = False
        for i in range(runs):
            rc = _run_pytest(tests_dir, solution[0][1], entry, timeout_s,
                             shuffle_seed=1000 + i)
            if rc != 0:
                flaky = True
                break
        result.gates.append(GateResult(
            "G5", f"no flakes ({runs}x)", not flaky,
            f"{runs} shuffled runs all passed" if not flaky
            else "a shuffled run failed (flaky)"))
    else:
        result.gates.append(GateResult("G5", f"no flakes ({runs}x)", False,
                                        "no solution/ to run"))

    return result

"""Run and validate BYO stages.

A stage's candidate directory (solution / alt / mutant / the learner's workspace)
is a *cumulative* codebase. Running "stage N" means running the ``tests/`` of
every stage ``1..N`` against that one directory, so earlier stages act as a
regression suite.

Validity gates per stage (docs/TESTING.md, mapped to BYO):

* G1 starter fails stage N (its own tests) — the stage isn't implemented yet.
* G2 solution passes stage N + all regressions (1..N).
* G3 every alt_solutions/* passes 1..N (>= 1 required; BYO alts are expensive).
* G4 every mutants/* fails >= 1 test in 1..N.
* G5 solution passes 1..N ``runs`` times with pytest-randomly, no flakes.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from de_cli.byo import Challenge, Stage
from de_cli.java_support import spark_env


@dataclass
class GateResult:
    gate: str
    name: str
    passed: bool
    detail: str = ""


@dataclass
class StageValidation:
    stage_id: str
    gates: list[GateResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(g.passed for g in self.gates)


def _tests_up_to(ch: Challenge, stage: Stage) -> list[Path]:
    """tests/ dirs for every stage number <= this stage's number (in order)."""
    dirs = []
    for s in ch.stages:
        if s.number <= stage.number:
            t = s.dir / "tests"
            if t.is_dir():
                dirs.append(t)
    return dirs


def run_stage(ch: Challenge, stage: Stage, submission_dir: Path, *,
              only_this_stage: bool = False, shuffle_seed: int | None = None,
              entry: str | None = None) -> int:
    """Run stage tests (cumulative by default) against ``submission_dir``.

    Returns the pytest exit code. ``only_this_stage`` runs just stage N's tests
    (used by G1). The learner code is exposed via ``DE_SUBMISSION_DIR`` exactly
    like exercises, so BYO tests use the ``submission`` fixture too (or import
    their own black-box client).
    """
    if only_this_stage:
        test_dirs = [stage.dir / "tests"]
    else:
        test_dirs = _tests_up_to(ch, stage)
    if not test_dirs:
        return 5  # pytest "no tests collected"
    env = spark_env(os.environ)
    env["DE_SUBMISSION_DIR"] = str(submission_dir)
    env["DE_SUBMISSION_ENTRY"] = entry or stage.meta.get("entry", "solution.py")
    env["DE_BYO_STAGE"] = str(stage.number)
    args = [
        sys.executable, "-m", "pytest", *[str(t) for t in test_dirs],
        "-q", "-p", "no:cacheprovider", f"--timeout={stage.timeout_s}",
    ]
    if shuffle_seed is None:
        args += ["-p", "no:randomly"]
    else:
        args += [f"--randomly-seed={shuffle_seed}"]
    proc = subprocess.run(args, env=env, capture_output=True, text=True,
                          timeout=stage.timeout_s + 60)
    return proc.returncode


def _candidate_dirs(stage: Stage, kind: str) -> list[tuple[str, Path]]:
    if kind in ("solution",):
        d = stage.dir / kind
        return [(kind, d)] if d.is_dir() else []
    parent = stage.dir / kind
    if not parent.is_dir():
        return []
    return [(f"{kind}/{p.name}", p) for p in sorted(parent.iterdir()) if p.is_dir()]


def validate_stage(ch: Challenge, stage: Stage, runs: int = 5) -> StageValidation:
    result = StageValidation(stage_id=stage.id)

    # G1: starter fails stage N (challenge-level cumulative starter).
    starter = ch.starter_dir
    if not starter.is_dir():
        result.gates.append(GateResult("G1", "starter fails", False,
                                        f"no starter/ at {starter}"))
    else:
        rc = run_stage(ch, stage, starter, only_this_stage=True)
        result.gates.append(GateResult(
            "G1", "starter fails", rc != 0,
            "starter failed stage N (good)" if rc != 0
            else "starter PASSED stage N (should fail)"))

    # G2: solution passes 1..N.
    solution = _candidate_dirs(stage, "solution")
    if not solution:
        result.gates.append(GateResult("G2", "solution passes", False,
                                        "no solution/"))
    else:
        rc = run_stage(ch, stage, solution[0][1])
        result.gates.append(GateResult(
            "G2", "solution passes (1..N)", rc == 0,
            "solution passed stage+regression" if rc == 0
            else "solution FAILED (rc!=0)"))

    # G3: every alt passes 1..N (>= 1 required).
    alts = _candidate_dirs(stage, "alt_solutions")
    if len(alts) < 1:
        result.gates.append(GateResult("G3", "alts pass (>=1)", False,
                                        "found 0 alts, need >= 1"))
    else:
        failures = [name for name, d in alts if run_stage(ch, stage, d) != 0]
        result.gates.append(GateResult(
            "G3", "alts pass (>=1)", not failures,
            "all alts passed" if not failures else f"failed: {failures}"))

    # G4: every mutant fails >= 1 test in 1..N (>= 3 required).
    mutants = _candidate_dirs(stage, "mutants")
    if len(mutants) < 3:
        result.gates.append(GateResult("G4", "mutants killed (>=3)", False,
                                        f"found {len(mutants)} mutant(s), need >= 3"))
    else:
        survivors = [name for name, d in mutants if run_stage(ch, stage, d) == 0]
        result.gates.append(GateResult(
            "G4", "mutants killed (>=3)", not survivors,
            "all mutants killed" if not survivors else f"survived: {survivors}"))

    # G5: solution passes 1..N `runs` times with shuffling.
    if solution:
        flaky = False
        for i in range(runs):
            if run_stage(ch, stage, solution[0][1], shuffle_seed=2000 + i) != 0:
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

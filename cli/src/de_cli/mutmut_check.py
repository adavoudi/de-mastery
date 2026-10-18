"""Secondary G4 check (docs/TESTING.md): run ``mutmut`` on reference solutions and
report surviving automatic mutants. This is NON-BLOCKING — it always returns 0, so
it is advisory. Surviving mutants are reviewed by a human: either a test
is added, or the mutant is marked equivalent.

mutmut 3.x mutates a module and runs a test command per mutant. Driving it per
exercise is expensive, so this runs on demand (``de validate --mutmut [target]``)
rather than as part of the normal gates.

Because our exercise tests import the submission via ``DE_SUBMISSION_DIR``, we point
mutmut at a copy of the exercise's ``solution/`` and run the exercise's own tests
against it. We generate a tiny ``setup.cfg``-style config in a temp dir.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from rich.console import Console

from de_cli import content as content_mod
from de_cli.content import Exercise
from de_cli.java_support import spark_env
from de_cli.paths import content_root

console = Console()


def _mutmut_one(ex: Exercise) -> tuple[int, int]:
    """Run mutmut over one exercise's solution. Returns (killed, survived)."""
    sol = ex.dir / "solution"
    tests = ex.dir / "tests"
    entry = ex.entry[0] if isinstance(ex.entry, list) else ex.entry
    if not sol.is_dir() or not tests.is_dir():
        return (0, 0)

    work = Path(tempfile.mkdtemp(prefix="mutmut_"))
    try:
        pkg = work / "sol"
        shutil.copytree(sol, pkg)
        runner = (
            f"{sys.executable} -m pytest {tests} -q -p no:cacheprovider "
            f"-p no:randomly --timeout={ex.timeout_s}"
        )
        setup = work / "setup.cfg"
        setup.write_text(
            "[mutmut]\n"
            f"source_paths={pkg / entry}\n"
            f"runner={runner}\n"
        )
        env = spark_env(os.environ)
        env["DE_SUBMISSION_DIR"] = str(pkg)
        env["DE_SUBMISSION_ENTRY"] = entry
        proc = subprocess.run(
            [sys.executable, "-m", "mutmut", "run"],
            cwd=work, env=env, capture_output=True, text=True, timeout=ex.timeout_s * 20,
        )
        out = proc.stdout + proc.stderr
        # mutmut 3.x prints a results summary; surface it verbatim.
        console.print(f"[dim]{ex.id}: {out.strip().splitlines()[-5:]}[/]")
        killed = out.count("🎉") + out.lower().count("killed")
        survived = out.count("🙁") + out.lower().count("survived")
        return (killed, survived)
    except Exception as exc:  # non-blocking
        console.print(f"[yellow]mutmut skipped for {ex.id}: {exc}[/]")
        return (0, 0)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def run(target: str | None, content_root_opt: Path | None) -> int:
    croot = content_root_opt if content_root_opt else content_root()
    exercises = []
    for m in content_mod.discover_modules(croot):
        for ex in m.exercises:
            if target is None or target in (m.id, ex.id):
                exercises.append(ex)
    if not exercises:
        console.print(f"[yellow]No exercises for mutmut (target={target!r}).[/]")
        return 0
    total_survived = 0
    for ex in exercises:
        killed, survived = _mutmut_one(ex)
        total_survived += survived
        tag = "[green]" if survived == 0 else "[yellow]"
        console.print(f"{tag}mutmut {ex.id}: killed≈{killed} survived≈{survived}[/]")
    console.print(
        f"[bold]mutmut summary:[/] ~{total_survived} surviving mutant(s) "
        "(advisory; review each — add a test or mark equivalent)."
    )
    return 0  # always non-blocking

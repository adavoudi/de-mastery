"""The `de` command-line tool."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from de_cli import content as content_mod
from de_cli import byo as byo_mod
from de_cli import byo_runner as byo_runner_mod
from de_cli import doctor as doctor_mod
from de_cli import plan as plan_mod
from de_cli import progress as progress_mod
from de_cli import quiz as quiz_mod
from de_cli.paths import content_root, learner_path, repo_root
from de_cli.runner import copy_starter, run_tests, workspace_dir_for
from de_cli.validate import validate_exercise

app = typer.Typer(
    help="de — the DE Mastery learning platform CLI.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()

lab_app = typer.Typer(help="Start/stop Docker Compose lab profiles.", no_args_is_help=True)
app.add_typer(lab_app, name="lab")


# ---------------------------------------------------------------------------
# ordering helpers
# ---------------------------------------------------------------------------
def _ordered_exercises(croot: Path | None = None):
    """Exercises ordered by (track order from learner.yaml, module id, exercise dir)."""
    import yaml
    root = repo_root()
    order = ["FND", "SPK", "LKH", "JVM", "KFK", "FLK", "STH"]
    lp = learner_path(root)
    if lp.exists():
        learner = yaml.safe_load(lp.read_text()) or {}
        order = ((learner.get("tracks") or {}).get("order")) or order
    rank = {t: i for i, t in enumerate(order)}
    modules = content_mod.discover_modules(croot)
    modules.sort(key=lambda m: (rank.get(m.track, 99), m.id))
    out = []
    for m in modules:
        for ex in m.exercises:
            out.append(ex)
    return out


def _prereqs_met(ex, croot: Path | None = None) -> tuple[bool, list[str]]:
    """All exercises of required modules must be passed."""
    mod = content_mod.find_module(ex.module_id, croot)
    if not mod or not mod.requires:
        return True, []
    data = progress_mod.load()
    missing = []
    for req in mod.requires:
        reqmod = content_mod.find_module(req, croot)
        if not reqmod or not reqmod.exercises:
            missing.append(req)
            continue
        for rex in reqmod.exercises:
            if data["exercises"].get(rex.id, {}).get("status") != "passed":
                missing.append(req)
                break
    return (not missing), missing


# ---------------------------------------------------------------------------
# BYO helpers
# ---------------------------------------------------------------------------
def _is_byo_id(target: str) -> bool:
    """A BYO challenge id (``SPK-BYO``) or stage id (``SPK-BYO/03``)."""
    head = target.split("/", 1)[0]
    return head.endswith("-BYO")


def _byo_workspace_dir(ch: byo_mod.Challenge, root: Path | None = None) -> Path:
    """Cumulative learner codebase lives at ``workspace/byo/<challenge-id>/``."""
    from de_cli.paths import workspace_root
    return workspace_root(root) / "byo" / ch.id


def _byo_copy_starter(ch: byo_mod.Challenge, overwrite: bool = False) -> Path:
    import shutil
    dest = _byo_workspace_dir(ch)
    if dest.exists() and not overwrite:
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ch.starter_dir, dest)
    return dest



# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
@app.command()
def status():
    """Progress by track and overall."""
    data = progress_mod.load()
    exercises = _ordered_exercises()
    table = Table(title="Progress by track")
    table.add_column("Track")
    table.add_column("Passed", justify="right")
    table.add_column("Total", justify="right")
    by_track: dict[str, list] = {}
    for ex in exercises:
        by_track.setdefault(ex.module_id.split("-")[0], []).append(ex)
    if not exercises:
        console.print("[yellow]No content modules found yet. "
                      "Content is built by other phases.[/]")
    for track, exs in by_track.items():
        passed = sum(1 for e in exs
                     if data["exercises"].get(e.id, {}).get("status") == "passed")
        table.add_row(track, str(passed), str(len(exs)))
    # BYO challenges as their own rows (<track>-BYO).
    for ch in byo_mod.discover_challenges():
        passed = sum(1 for st in ch.stages
                     if data["exercises"].get(st.id, {}).get("status") == "passed")
        if ch.stages:
            table.add_row(ch.id, str(passed), str(len(ch.stages)))
    console.print(table)
    cur = data.get("current")
    if cur:
        console.print(f"Current: [bold]{cur}[/]")


# ---------------------------------------------------------------------------
# web
# ---------------------------------------------------------------------------
@app.command()
def web(host: str = typer.Option("127.0.0.1", help="Interface to bind."),
        port: int = typer.Option(8000, help="Port to listen on.")):
    """Serve the course (lessons, exercises, quizzes) as a website on this machine."""
    try:
        import uvicorn
        from de_web.app import create_app
    except ImportError:
        console.print("[red]The website needs extra packages.[/] Run: [bold]uv sync --extra web[/]")
        raise typer.Exit(1)
    console.print(f"Course site: [bold]http://{host}:{port}[/]  (Ctrl+C to stop)")
    uvicorn.run(create_app(repo_root()), host=host, port=port, log_level="warning")


# ---------------------------------------------------------------------------
# next
# ---------------------------------------------------------------------------
@app.command()
def next():
    """Show the next unfinished exercise, respecting `requires`."""
    data = progress_mod.load()
    for ex in _ordered_exercises():
        st = data["exercises"].get(ex.id, {}).get("status")
        if st == "passed":
            continue
        ok, missing = _prereqs_met(ex)
        if not ok:
            continue
        console.print(f"Next: [bold cyan]{ex.id}[/] — {ex.meta.get('title', '')}")
        console.print(f"  start it with: [green]de start {ex.id}[/]")
        return
    # Then BYO stages, in order; stage N is suggested once stage N-1 has passed.
    for ch in byo_mod.discover_challenges():
        prev_ok = True
        for st in ch.stages:
            st_status = data["exercises"].get(st.id, {}).get("status")
            if st_status == "passed":
                continue
            if not prev_ok:
                break
            console.print(f"Next (BYO): [bold cyan]{st.id}[/] — {st.title}")
            console.print(f"  start it with: [green]de start {st.id}[/]")
            return
    console.print("[green]Nothing pending (or no content yet). 🎉[/]")


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------
@app.command()
def start(exercise_id: str):
    """Copy a starter into workspace/ and set it as current (exercise OR BYO stage)."""
    if _is_byo_id(exercise_id):
        found = byo_mod.find_stage(exercise_id) or (
            None if "/" in exercise_id else _first_byo_stage(exercise_id))
        if not found:
            console.print(f"[red]BYO stage not found:[/] {exercise_id}")
            raise typer.Exit(1)
        ch, st = found
        dest = _byo_copy_starter(ch)
        progress_mod.record_start(st.id)
        console.print(f"Started [bold]{st.id}[/] — {st.title}")
        console.print(f"Edit the cumulative codebase in: [green]{dest}[/]")
        console.print("Run tests (stage N + regressions) with: [green]de test[/]")
        return
    ex = content_mod.find_exercise(exercise_id)
    if not ex:
        console.print(f"[red]Exercise not found:[/] {exercise_id}")
        raise typer.Exit(1)
    ok, missing = _prereqs_met(ex)
    if not ok:
        console.print(f"[yellow]Warning: prerequisites not met:[/] {missing}")
    dest = copy_starter(ex)
    progress_mod.record_start(ex.id)
    console.print(f"Started [bold]{ex.id}[/]. Edit files in: [green]{dest}[/]")
    console.print(f"Run tests with: [green]de test[/]")


def _first_byo_stage(challenge_id: str):
    ch = byo_mod.find_challenge(challenge_id)
    if ch and ch.stages:
        return ch, ch.stages[0]
    return None


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------
@app.command()
def test(exercise_id: Optional[str] = typer.Argument(None)):
    """Run tests for the current (or given) exercise; records the result."""
    data = progress_mod.load()
    ex_id = exercise_id or data.get("current")
    if not ex_id:
        console.print("[red]No current exercise. Use `de start <ID>` first.[/]")
        raise typer.Exit(1)
    if _is_byo_id(ex_id):
        found = byo_mod.find_stage(ex_id)
        if not found:
            console.print(f"[red]BYO stage not found:[/] {ex_id}")
            raise typer.Exit(1)
        ch, st = found
        sub = _byo_workspace_dir(ch)
        if not sub.exists():
            console.print("[yellow]No workspace copy; copying starter.[/]")
            sub = _byo_copy_starter(ch)
        rc = byo_runner_mod.run_stage(ch, st, sub)
        passed = rc == 0
        progress_mod.record_attempt(st.id, passed=passed)
        if passed:
            console.print("[green]PASSED ✓[/] (stage + regressions)")
        else:
            state = progress_mod.exercise_state(st.id)
            console.print(f"[red]FAILED ✗[/] (attempt {state.get('attempts')})")
        raise typer.Exit(0 if passed else 1)
    ex = content_mod.find_exercise(ex_id)
    if not ex:
        console.print(f"[red]Exercise not found:[/] {ex_id}")
        raise typer.Exit(1)
    sub = workspace_dir_for(ex)
    if not sub.exists():
        console.print(f"[yellow]No workspace copy; copying starter.[/]")
        sub = copy_starter(ex)
    rc = run_tests(ex, sub)
    passed = rc == 0
    progress_mod.record_attempt(ex.id, passed=passed)
    if passed:
        console.print("[green]PASSED ✓[/]")
    else:
        st = progress_mod.exercise_state(ex.id)
        console.print(f"[red]FAILED ✗[/] (attempt {st.get('attempts')})")
    raise typer.Exit(0 if passed else 1)


# ---------------------------------------------------------------------------
# hint
# ---------------------------------------------------------------------------
@app.command()
def hint(level: int = typer.Option(1, min=1, max=3),
         exercise_id: Optional[str] = typer.Argument(None)):
    """Show a progressive hint (levels 1-3)."""
    data = progress_mod.load()
    ex_id = exercise_id or data.get("current")
    ex = content_mod.find_exercise(ex_id) if ex_id else None
    if not ex:
        console.print("[red]No exercise selected.[/]")
        raise typer.Exit(1)
    hints = ex.hints
    if len(hints) < level:
        console.print(f"[yellow]Only {len(hints)} hint(s) available.[/]")
        raise typer.Exit(1)
    for i in range(level):
        console.print(f"[cyan]Hint {i + 1}:[/] {hints[i]}")


# ---------------------------------------------------------------------------
# solution
# ---------------------------------------------------------------------------
@app.command()
def solution(exercise_id: Optional[str] = typer.Argument(None)):
    """Show the reference solution. Unlocks after a pass or 3 failed attempts."""
    data = progress_mod.load()
    ex_id = exercise_id or data.get("current")
    ex = content_mod.find_exercise(ex_id) if ex_id else None
    if not ex:
        console.print("[red]No exercise selected.[/]")
        raise typer.Exit(1)
    st = data["exercises"].get(ex.id, {})
    unlocked = st.get("status") == "passed" or st.get("attempts", 0) >= 3
    if not unlocked:
        console.print(
            f"[yellow]Locked.[/] Solution unlocks after a pass or 3 failed attempts "
            f"(you have {st.get('attempts', 0)}).")
        raise typer.Exit(1)
    sol_dir = ex.dir / "solution"
    for f in sorted(sol_dir.iterdir()):
        if f.is_file():
            console.rule(f.name)
            console.print(f.read_text())


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
@app.command()
def validate(
    target: Optional[str] = typer.Argument(
        None, help="Module/exercise ID, BYO challenge/stage ID, or omitted for all."),
    content_root_opt: Optional[Path] = typer.Option(
        None, "--content-root", help="Content dir to validate (default: ./content)."),
    byo_flag: bool = typer.Option(
        False, "--byo", help="Validate BYO challenges (under ./byo) instead of content."),
    byo_root_opt: Optional[Path] = typer.Option(
        None, "--byo-root", help="BYO dir to validate (default: ./byo)."),
    changed_ref: Optional[str] = typer.Option(
        None, "--changed", help="Only validate exercises/challenges changed since "
        "this git ref (e.g. HEAD or main)."),
    mutmut_flag: bool = typer.Option(
        False, "--mutmut", help="Secondary G4: run mutmut on reference solutions "
        "(non-blocking report of surviving automatic mutants)."),
    runs: int = typer.Option(5, help="G5 shuffled runs."),
):
    """Run validity gates G1-G5 and print a table; exit 1 if any gate fails.

    Validates ``content/`` exercises by default. Pass ``--byo`` (optionally with a
    challenge/stage ``target`` like ``SPK-BYO`` or ``SPK-BYO/03``) to validate BYO
    stages instead. A BYO ``target`` auto-enables BYO mode. ``--changed <ref>``
    restricts to what git shows as changed. ``--mutmut`` runs the secondary
    automatic-mutation check from docs/TESTING.md (non-blocking).
    """
    if mutmut_flag:
        from de_cli import mutmut_check
        rc = mutmut_check.run(target, content_root_opt)
        raise typer.Exit(rc)

    is_byo = byo_flag or byo_root_opt or (target and _is_byo_id(target))
    if changed_ref:
        from de_cli import changed as changed_mod
        if is_byo:
            ids = changed_mod.changed_challenge_ids(changed_ref, byo_root_opt)
            if not ids:
                console.print(f"[green]No BYO challenges changed since "
                              f"{changed_ref}.[/]")
                raise typer.Exit(0)
            any_fail = False
            for cid in ids:
                if _validate_byo(cid, byo_root_opt, runs):
                    any_fail = True
        else:
            ids = changed_mod.changed_exercise_ids(changed_ref, content_root_opt)
            if not ids:
                console.print(f"[green]No exercises changed since "
                              f"{changed_ref}.[/]")
                raise typer.Exit(0)
            any_fail = False
            for xid in ids:
                if _validate_content(xid, content_root_opt, runs):
                    any_fail = True
    elif is_byo:
        any_fail = _validate_byo(target, byo_root_opt, runs)
    else:
        any_fail = _validate_content(target, content_root_opt, runs)
    if any_fail:
        console.print("[red]Validation failed.[/]")
        raise typer.Exit(1)
    console.print("[green]All gates passed.[/]")


def _validate_content(target, content_root_opt, runs) -> bool:
    croot = content_root_opt if content_root_opt else content_root()
    modules = content_mod.discover_modules(croot)
    exercises = []
    for m in modules:
        for ex in m.exercises:
            if target is None or target in (m.id, ex.id):
                exercises.append(ex)
    if not exercises:
        console.print(f"[red]No exercises found[/] under {croot} "
                      f"(target={target!r}).")
        raise typer.Exit(1)
    any_fail = False
    for ex in exercises:
        result = validate_exercise(ex, runs=runs)
        _print_gate_table(f"validate {ex.id}", result.gates)
        if not result.ok:
            any_fail = True
    return any_fail


def _validate_byo(target, byo_root_opt, runs) -> bool:
    challenges = byo_mod.discover_challenges(byo_root_opt)
    pairs: list[tuple] = []
    for ch in challenges:
        for st in ch.stages:
            if target is None or target in (ch.id, st.id, st.short_id):
                pairs.append((ch, st))
    if not pairs:
        console.print(f"[red]No BYO stages found[/] (target={target!r}).")
        raise typer.Exit(1)
    any_fail = False
    for ch, st in pairs:
        result = byo_runner_mod.validate_stage(ch, st, runs=runs)
        _print_gate_table(f"validate {st.id}", result.gates)
        if not result.ok:
            any_fail = True
    return any_fail


def _print_gate_table(title: str, gates) -> None:
    table = Table(title=title)
    table.add_column("Gate")
    table.add_column("Check")
    table.add_column("Result")
    table.add_column("Detail")
    for g in gates:
        mark = "[green]PASS[/]" if g.passed else "[red]FAIL[/]"
        table.add_row(g.gate, g.name, mark, g.detail)
    console.print(table)


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------
@app.command()
def plan():
    """Recalculate the schedule from learner.yaml and module/syllabus hours."""
    res = plan_mod.compute_plan()
    table = Table(title="Study plan")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total hours (all tracks)", f"{res.total_hours:g}")
    table.add_row("Remaining hours", f"{res.remaining_hours:g}")
    table.add_row("Pace (h/week)", f"{res.hours_per_week:g}")
    if res.target_date:
        table.add_row("Target date", res.target_date.isoformat())
        table.add_row("Required h/week", f"{res.required_per_week:.1f}")
        table.add_row("Feasible", "yes" if res.feasible else "NO")
    else:
        table.add_row("Projected finish", res.projected_finish.isoformat())
    console.print(table)
    style = "green" if (res.feasible is None or res.feasible) else "yellow"
    console.print(f"[{style}]{res.message}[/]")


# ---------------------------------------------------------------------------
# quiz
# ---------------------------------------------------------------------------
@app.command()
def quiz(module_id: str,
         content_root_opt: Optional[Path] = typer.Option(None, "--content-root")):
    """Run a module quiz interactively and record the best score."""
    croot = content_root_opt if content_root_opt else content_root()
    mod = content_mod.find_module(module_id, croot)
    if not mod:
        console.print(f"[red]Module not found:[/] {module_id}")
        raise typer.Exit(1)
    quiz_file = mod.dir / "quiz.yaml"
    if not quiz_file.exists():
        console.print(f"[red]No quiz.yaml for[/] {module_id}")
        raise typer.Exit(1)
    questions = quiz_mod.load_quiz(quiz_file)
    responses: dict[str, list] = {}
    for q in questions:
        console.rule(q.id)
        console.print(q.prompt)
        for k, v in q.options.items():
            console.print(f"  [bold]{k}[/]: {v}")
        kind = "comma-separated" if q.type == "multi" else "one letter"
        raw = typer.prompt(f"Your answer ({kind})")
        responses[q.id] = [s.strip().upper() for s in raw.split(",") if s.strip()]
    score = quiz_mod.grade(questions, responses)
    progress_mod.record_quiz(module_id, score, quiz_mod.new_run_id())
    console.print(f"Score: [bold]{score * 100:.0f}%[/]")
    for q in questions:
        given = set(responses.get(q.id, []))
        ok = given == set(q.answer)
        mark = "[green]✓[/]" if ok else "[red]✗[/]"
        console.print(f"{mark} {q.id}: answer={q.answer} — {q.explanation}")
        if q.source:
            console.print(f"    source: {q.source}")


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------
@app.command()
def doctor():
    """Check python/uv/java/docker/RAM and print fixes."""
    checks = doctor_mod.run_all()
    table = Table(title="de doctor")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")
    for c in checks:
        mark = "[green]OK[/]" if c.ok else "[red]FAIL[/]"
        table.add_row(c.name, mark, c.detail)
    console.print(table)
    notes = [c for c in checks if c.fix]
    if notes:
        console.rule("Notes & fixes")
        for c in notes:
            label = c.name if not c.ok else f"{c.name} (ok, note)"
            console.print(f"[yellow]{label}:[/] {c.fix}")
    # doctor is informational; exit 0 unless python/uv broken (hard blockers).
    hard = [c for c in checks if not c.ok and c.name in ("python", "uv")]
    raise typer.Exit(1 if hard else 0)


# ---------------------------------------------------------------------------
# lab up/down/status
# ---------------------------------------------------------------------------
def _compose_base() -> list[str]:
    root = repo_root()
    compose = root / "labs" / "compose.yaml"
    return ["docker", "compose", "-f", str(compose)]


@lab_app.command("up")
def lab_up(profile: str):
    """`docker compose --profile <p> up -d`."""
    cmd = _compose_base() + ["--profile", profile, "up", "-d"]
    console.print(f"[dim]{' '.join(cmd)}[/]")
    raise typer.Exit(subprocess.call(cmd))


@lab_app.command("down")
def lab_down(profile: str):
    """`docker compose --profile <p> down`."""
    cmd = _compose_base() + ["--profile", profile, "down"]
    console.print(f"[dim]{' '.join(cmd)}[/]")
    raise typer.Exit(subprocess.call(cmd))


@lab_app.command("status")
def lab_status(profile: str):
    """`docker compose --profile <p> ps`."""
    cmd = _compose_base() + ["--profile", profile, "ps"]
    console.print(f"[dim]{' '.join(cmd)}[/]")
    raise typer.Exit(subprocess.call(cmd))


if __name__ == "__main__":
    app()

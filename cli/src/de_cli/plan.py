"""Scheduling (`de plan`), per docs/PLATFORM.md 'Scheduling'.

Hours per module come from built modules' ``module.yaml`` (authoritative) and, for
not-yet-built modules, from the ``h`` column of the ``tracks/*.md`` syllabi (fallback).

Behaviour:
* No target date  -> project a finish date from the pace.
* Target date set -> compute required hours/week; if it exceeds the pace, flag it and
  suggest skipping/reordering.
* Past progress is never lost: completed modules are subtracted from remaining work;
  measured minutes refine nothing destructively (they only inform, not overwrite).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

from de_cli import progress as progress_mod
from de_cli.content import discover_modules
from de_cli.paths import learner_path, repo_root
from de_cli.tracks import track_hours


@dataclass
class PlanResult:
    total_hours: float
    remaining_hours: float
    hours_per_week: float
    target_date: date | None
    projected_finish: date | None
    required_per_week: float | None
    feasible: bool | None
    message: str
    per_track: dict[str, float]


def _load_learner(root: Path) -> dict:
    p = learner_path(root)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text()) or {}


def _module_hours(root: Path) -> dict[str, float]:
    """Merge built-module hours (authoritative) over syllabi fallback hours, and
    add BYO challenge hours (keyed by challenge id, e.g. ``SPK-BYO``)."""
    hours = dict(track_hours(root))          # fallback for everything in syllabi
    for m in discover_modules(root / "content"):  # built modules override
        if m.hours:
            hours[m.id] = m.hours
    from de_cli import byo as byo_mod
    for ch in byo_mod.discover_challenges(root):
        if ch.hours:
            hours[ch.id] = ch.hours
    return hours


def _completed_modules(root: Path) -> set[str]:
    """A module counts as completed when every known exercise under it has passed.

    With no exercises recorded we treat the module as not completed. This is a simple,
    conservative rule for Phase 1.
    """
    data = progress_mod.load(root)
    passed_by_module: dict[str, bool] = {}
    for ex_id, st in data.get("exercises", {}).items():
        mod = ex_id.split("/")[0]
        ok = st.get("status") == "passed"
        passed_by_module[mod] = passed_by_module.get(mod, True) and ok
    return {m for m, ok in passed_by_module.items() if ok}


def compute_plan(root: Path | None = None, today: date | None = None) -> PlanResult:
    root = root or repo_root()
    today = today or date.today()
    learner = _load_learner(root)
    pace = (learner.get("pace") or {})
    tracks_cfg = (learner.get("tracks") or {})

    hpw = float(pace.get("hours_per_week") or 0) or 1.0
    skip = set(tracks_cfg.get("skip_modules") or [])

    hours = _module_hours(root)
    for mid in skip:
        hours.pop(mid, None)

    total = sum(hours.values())

    completed = _completed_modules(root)
    remaining_hours = sum(h for mid, h in hours.items() if mid not in completed)

    per_track: dict[str, float] = {}
    for mid, h in hours.items():
        if mid in completed:
            continue
        track = mid.split("-")[0]
        per_track[track] = per_track.get(track, 0.0) + h

    target_raw = pace.get("target_date")
    target: date | None = None
    if target_raw:
        if isinstance(target_raw, date):
            target = target_raw
        else:
            target = datetime.strptime(str(target_raw), "%Y-%m-%d").date()

    if target is None:
        weeks = remaining_hours / hpw if hpw else 0
        projected = today + timedelta(weeks=weeks)
        return PlanResult(
            total_hours=total, remaining_hours=remaining_hours, hours_per_week=hpw,
            target_date=None, projected_finish=projected, required_per_week=None,
            feasible=None,
            message=(f"No target date. At {hpw:g} h/week you finish the remaining "
                     f"{remaining_hours:g} h in ~{weeks:.1f} weeks "
                     f"(≈ {projected.isoformat()})."),
            per_track=per_track,
        )

    days_left = (target - today).days
    weeks_left = max(days_left / 7.0, 0.0)
    if weeks_left <= 0:
        required = float("inf")
        feasible = False
        message = (f"Target date {target.isoformat()} is in the past or today. "
                   f"{remaining_hours:g} h still remain.")
    else:
        required = remaining_hours / weeks_left
        feasible = required <= hpw + 1e-9
        if feasible:
            message = (f"Feasible. {remaining_hours:g} h over {weeks_left:.1f} weeks "
                       f"needs {required:.1f} h/week; your pace is {hpw:g} h/week.")
        else:
            over = required - hpw
            # Suggest the heaviest later tracks to trim.
            heavy = sorted(per_track.items(), key=lambda kv: kv[1], reverse=True)
            suggest = ", ".join(f"{t} ({h:g} h)" for t, h in heavy[:3])
            message = (f"Not feasible at {hpw:g} h/week. You need {required:.1f} h/week "
                       f"({over:.1f} h/week more). Consider skipping/reordering heavy "
                       f"tracks: {suggest}, or move the date out.")

    return PlanResult(
        total_hours=total, remaining_hours=remaining_hours, hours_per_week=hpw,
        target_date=target, projected_finish=None, required_per_week=required,
        feasible=feasible, message=message, per_track=per_track,
    )

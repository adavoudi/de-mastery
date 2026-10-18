"""Read/write ``progress.json`` (local, gitignored learner state)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from de_cli.paths import progress_path

VERSION = 1


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load(root: Path | None = None) -> dict:
    p = progress_path(root)
    if not p.exists():
        return {"version": VERSION, "exercises": {}, "quizzes": {}, "current": None}
    data = json.loads(p.read_text())
    data.setdefault("version", VERSION)
    data.setdefault("exercises", {})
    data.setdefault("quizzes", {})
    data.setdefault("current", None)
    return data


def save(data: dict, root: Path | None = None) -> None:
    p = progress_path(root)
    p.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def record_start(exercise_id: str, root: Path | None = None) -> dict:
    data = load(root)
    ex = data["exercises"].setdefault(exercise_id, {})
    ex.setdefault("status", "started")
    ex.setdefault("attempts", 0)
    ex.setdefault("first_started", _now())
    ex.setdefault("minutes", 0)
    data["current"] = exercise_id
    save(data, root)
    return data


def record_attempt(exercise_id: str, passed: bool, minutes: int = 0,
                   root: Path | None = None) -> dict:
    data = load(root)
    ex = data["exercises"].setdefault(exercise_id, {
        "status": "started", "attempts": 0, "first_started": _now(), "minutes": 0,
    })
    ex["attempts"] = ex.get("attempts", 0) + 1
    ex["minutes"] = ex.get("minutes", 0) + max(0, minutes)
    if passed:
        ex["status"] = "passed"
        ex.setdefault("passed_at", _now())
    else:
        if ex.get("status") != "passed":
            ex["status"] = "failed"
    data["current"] = exercise_id
    save(data, root)
    return data


def record_quiz(module_id: str, score: float, quiz_run_id: str,
                root: Path | None = None) -> dict:
    data = load(root)
    q = data["quizzes"].setdefault(module_id, {"best_score": 0.0, "taken": []})
    q["best_score"] = max(q.get("best_score", 0.0), round(score, 4))
    q.setdefault("taken", []).append(quiz_run_id)
    save(data, root)
    return data


def exercise_state(exercise_id: str, root: Path | None = None) -> dict:
    return load(root)["exercises"].get(exercise_id, {})

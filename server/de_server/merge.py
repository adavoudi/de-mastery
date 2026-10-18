"""Validation and merge of progress payloads.

The merge is monotonic, commutative and idempotent, so syncing twice, or from two machines
in any order, converges: ``passed`` is never lost, counters only grow, the earliest start and
pass timestamps win, and the best quiz score wins.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-/]{0,119}$")
_RANK = {"started": 0, "failed": 1, "passed": 2}
MAX_ITEMS = 5000


def _check_id(v: str) -> str:
    if not _ID_RE.match(v):
        raise ValueError("invalid id")
    return v


def _check_ts(v: str | None) -> str | None:
    if v is None:
        return None
    datetime.fromisoformat(v)   # raises ValueError if malformed
    if len(v) > 40:
        raise ValueError("timestamp too long")
    return v


class ExerciseIn(BaseModel):
    status: Literal["started", "failed", "passed"]
    attempts: int = Field(0, ge=0, le=1_000_000)
    minutes: int = Field(0, ge=0, le=10_000_000)
    first_started: str | None = None
    passed_at: str | None = None

    _ts = field_validator("first_started", "passed_at")(_check_ts)


class QuizIn(BaseModel):
    best_score: float = Field(ge=0.0, le=1.0)


class ProgressIn(BaseModel):
    version: int = 1
    exercises: dict[str, ExerciseIn] = Field(default_factory=dict)
    quizzes: dict[str, QuizIn] = Field(default_factory=dict)
    current: str | None = None

    @field_validator("exercises", "quizzes")
    @classmethod
    def _keys(cls, v):
        if len(v) > MAX_ITEMS:
            raise ValueError("too many entries")
        for k in v:
            _check_id(k)
        return v

    @field_validator("current")
    @classmethod
    def _current(cls, v):
        return None if v is None else _check_id(v)


def _min_ts(a: str | None, b: str | None) -> str | None:
    if a and b:
        return min(a, b)
    return a or b


def merge_into(conn: sqlite3.Connection, user_id: int, payload: ProgressIn) -> None:
    for ex_id, new in payload.exercises.items():
        row = conn.execute(
            "SELECT * FROM exercises WHERE user_id=? AND exercise_id=?", (user_id, ex_id)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO exercises VALUES (?,?,?,?,?,?,?)",
                (user_id, ex_id, new.status, new.attempts, new.minutes,
                 new.first_started, new.passed_at),
            )
            continue
        status = row["status"] if _RANK[row["status"]] >= _RANK[new.status] else new.status
        conn.execute(
            "UPDATE exercises SET status=?, attempts=?, minutes=?, first_started=?, passed_at=? "
            "WHERE user_id=? AND exercise_id=?",
            (status, max(row["attempts"], new.attempts), max(row["minutes"], new.minutes),
             _min_ts(row["first_started"], new.first_started),
             _min_ts(row["passed_at"], new.passed_at), user_id, ex_id),
        )
    for module_id, q in payload.quizzes.items():
        conn.execute(
            "INSERT INTO quizzes VALUES (?,?,?) "
            "ON CONFLICT(user_id, module_id) DO UPDATE SET best_score=max(best_score, excluded.best_score)",
            (user_id, module_id, q.best_score),
        )
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn.execute(
        "INSERT INTO user_state(user_id, current, last_sync) VALUES (?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET current=COALESCE(excluded.current, current), "
        "last_sync=excluded.last_sync",
        (user_id, payload.current, now),
    )


def load_state(conn: sqlite3.Connection, user_id: int) -> dict:
    """The user's merged progress in ``progress.json`` shape."""
    exercises = {}
    for r in conn.execute("SELECT * FROM exercises WHERE user_id=?", (user_id,)):
        ex = {"status": r["status"], "attempts": r["attempts"], "minutes": r["minutes"]}
        if r["first_started"]:
            ex["first_started"] = r["first_started"]
        if r["passed_at"]:
            ex["passed_at"] = r["passed_at"]
        exercises[r["exercise_id"]] = ex
    quizzes = {r["module_id"]: {"best_score": r["best_score"]}
               for r in conn.execute("SELECT * FROM quizzes WHERE user_id=?", (user_id,))}
    st = conn.execute("SELECT current, last_sync FROM user_state WHERE user_id=?",
                      (user_id,)).fetchone()
    return {"version": 1, "exercises": exercises, "quizzes": quizzes,
            "current": st["current"] if st else None,
            "last_sync": st["last_sync"] if st else None}

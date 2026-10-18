"""Exercise catalog (per-track totals) from a checkout of the course repo."""

from __future__ import annotations

from pathlib import Path


def track_of(exercise_id: str) -> str:
    module = exercise_id.split("/")[0]
    return module if "BYO" in module else module.split("-")[0]


def load(repo_root: Path | None) -> dict[str, list[str]]:
    """{track: [exercise ids]} ; empty when no repo is configured."""
    if repo_root is None:
        return {}
    from de_cli import byo as byo_mod
    from de_cli import content as content_mod

    ids: list[str] = [e.id for e in content_mod.all_exercises(repo_root / "content")]
    for ch in byo_mod.discover_challenges(repo_root / "byo"):
        ids.extend(st.id for st in ch.stages)
    out: dict[str, list[str]] = {}
    for i in ids:
        out.setdefault(track_of(i), []).append(i)
    return out

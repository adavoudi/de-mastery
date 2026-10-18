"""Map files changed since a git ref to the exercise / BYO-stage IDs they affect.

Used by ``de validate --changed <ref>`` so only what changed gets validated.
For a BYO challenge, a change in ANY stage dir re-validates the whole challenge
(stages depend on each other through the cumulative regression suite).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from de_cli import byo as byo_mod
from de_cli import content as content_mod
from de_cli.paths import repo_root


def changed_files(ref: str, root: Path | None = None) -> list[Path]:
    root = root or repo_root()
    try:
        out = subprocess.run(
            ["git", "diff", "--name-only", f"{ref}...HEAD"],
            cwd=root, capture_output=True, text=True, timeout=30,
        )
        names = out.stdout.splitlines()
        # also include uncommitted working-tree changes
        out2 = subprocess.run(
            ["git", "diff", "--name-only", ref],
            cwd=root, capture_output=True, text=True, timeout=30,
        )
        names += out2.stdout.splitlines()
    except Exception:
        return []
    return [root / n for n in dict.fromkeys(n for n in names if n)]


def changed_exercise_ids(ref: str, content_dir: Path | None = None,
                         root: Path | None = None) -> list[str]:
    root = root or repo_root()
    files = changed_files(ref, root)
    ids: list[str] = []
    for ex in content_mod.all_exercises(content_dir):
        exdir = ex.dir.resolve()
        if any(_under(f, exdir) for f in files):
            ids.append(ex.id)
    return ids


def changed_challenge_ids(ref: str, byo_root: Path | None = None,
                          root: Path | None = None) -> list[str]:
    root = root or repo_root()
    files = changed_files(ref, root)
    ids: list[str] = []
    for ch in byo_mod.discover_challenges(byo_root):
        chdir = ch.dir.resolve()
        if any(_under(f, chdir) for f in files):
            ids.append(ch.id)
    return ids


def _under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent)
        return True
    except (ValueError, OSError):
        return False

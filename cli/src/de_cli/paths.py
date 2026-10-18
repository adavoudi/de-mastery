"""Repo path discovery for the CLI."""

from __future__ import annotations

from pathlib import Path


def repo_root(start: Path | None = None) -> Path:
    cur = (start or Path.cwd()).resolve()
    for p in [cur, *cur.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return cur


def content_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / "content"


def tracks_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / "tracks"


def progress_path(root: Path | None = None) -> Path:
    return (root or repo_root()) / "progress.json"


def workspace_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / "workspace"


def learner_path(root: Path | None = None) -> Path:
    """The learner's profile: ``learner.yaml`` (local, gitignored), else the tracked template."""
    r = root or repo_root()
    p = r / "learner.yaml"
    return p if p.exists() else r / "learner.example.yaml"

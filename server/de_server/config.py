"""Server settings, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_url: str                 # public URL, e.g. https://de.example.com (no trailing slash)
    session_secret: str           # signs the session cookie; keep stable across restarts
    github_client_id: str
    github_client_secret: str
    db_path: Path
    repo_root: Path | None = None  # a checkout of the course repo, for per-track totals

    @property
    def https(self) -> bool:
        return self.base_url.startswith("https://")


def from_env() -> Settings:
    def need(name: str) -> str:
        v = os.environ.get(name, "").strip()
        if not v:
            raise RuntimeError(f"missing required environment variable {name}")
        return v

    secret = need("DE_SESSION_SECRET")
    if len(secret) < 32:
        raise RuntimeError("DE_SESSION_SECRET must be at least 32 characters")
    repo = os.environ.get("DE_REPO_ROOT", "").strip()
    return Settings(
        base_url=need("DE_BASE_URL").rstrip("/"),
        session_secret=secret,
        github_client_id=need("GITHUB_CLIENT_ID"),
        github_client_secret=need("GITHUB_CLIENT_SECRET"),
        db_path=Path(os.environ.get("DE_DB_PATH", "de_server.sqlite3")),
        repo_root=Path(repo) if repo else None,
    )

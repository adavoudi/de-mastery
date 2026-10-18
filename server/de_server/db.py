"""SQLite storage. One connection per request; WAL mode so reads never block the writer."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    github_id   INTEGER NOT NULL UNIQUE,
    login       TEXT NOT NULL,
    avatar_url  TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_codes (
    device_hash TEXT PRIMARY KEY,           -- sha256 of the secret device_code
    user_code   TEXT NOT NULL UNIQUE,
    user_id     INTEGER REFERENCES users(id),   -- set when the user approves
    expires_at  REAL NOT NULL,
    last_poll   REAL NOT NULL DEFAULT 0,
    consumed    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tokens (
    id          INTEGER PRIMARY KEY,
    token_hash  TEXT NOT NULL UNIQUE,       -- sha256; the token itself is never stored
    user_id     INTEGER NOT NULL REFERENCES users(id),
    label       TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    last_used   TEXT,
    revoked     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS exercises (
    user_id       INTEGER NOT NULL REFERENCES users(id),
    exercise_id   TEXT NOT NULL,
    status        TEXT NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    minutes       INTEGER NOT NULL DEFAULT 0,
    first_started TEXT,
    passed_at     TEXT,
    PRIMARY KEY (user_id, exercise_id)
);
CREATE TABLE IF NOT EXISTS quizzes (
    user_id    INTEGER NOT NULL REFERENCES users(id),
    module_id  TEXT NOT NULL,
    best_score REAL NOT NULL,
    PRIMARY KEY (user_id, module_id)
);
CREATE TABLE IF NOT EXISTS user_state (
    user_id    INTEGER PRIMARY KEY REFERENCES users(id),
    current    TEXT,
    last_sync  TEXT
);
"""


def connect(path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init(path: Path | str) -> None:
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

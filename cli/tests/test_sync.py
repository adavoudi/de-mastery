"""``de login`` / ``de sync`` against the real server app, in-process."""

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parents[2] / "server" / "tests"))

from conftest_server import FakeGitHub  # noqa: E402
from de_cli import progress as P  # noqa: E402
from de_cli import sync as S  # noqa: E402
from de_server import app as app_mod  # noqa: E402
from de_server.config import Settings  # noqa: E402

SERVER = "http://localhost:8100"


@pytest.fixture
def env(tmp_path, monkeypatch, temp_repo):
    monkeypatch.setenv("DE_CONFIG_DIR", str(tmp_path / "cfg"))
    monkeypatch.setattr(app_mod, "POLL_INTERVAL_S", 0)
    gh = FakeGitHub()
    settings = Settings(SERVER, "x" * 40, "id", "secret", tmp_path / "s.sqlite3")
    web = TestClient(app_mod.create_app(settings, gh), follow_redirects=False)
    cli = TestClient(web.app, follow_redirects=False)     # the CLI has its own (empty) cookie jar

    def fake_request(method, url, body=None, token=None):
        r = cli.request(method, url.replace(SERVER, ""), json=body,
                        headers={"Authorization": f"Bearer {token}"} if token else {})
        return r.status_code, r.json()

    monkeypatch.setattr(S, "_request", fake_request)
    return web, temp_repo


def _approve(web):
    """Approve whatever device code is pending, as the logged-in browser user."""
    r = web.get("/auth/login")
    state = r.headers["location"].split("state=")[1]
    web.get(f"/auth/github/callback?code=ok&state={state}")
    csrf = web.get("/device").text.split('name="csrf" value="')[1].split('"')[0]
    return csrf


def test_login_then_sync_roundtrip(env, tmp_path):
    web, repo = env
    P.record_attempt("FND-01/01-a", passed=True, minutes=10)
    P.record_quiz("FND-01", 0.9, "run-1")

    def sleep(_):                      # the "user" approves while the CLI is waiting
        pending = web.app.router  # noqa: F841 - keep reference
        code = shown[-1]
        csrf = _approve(web)
        web.post("/device", data={"user_code": code, "csrf": csrf})

    shown: list[str] = []

    def say(msg):
        if "approve this code" in msg:
            shown.append(msg.split(":")[-1].strip())

    assert S.login(SERVER, say=say, sleep=sleep) == "alice"
    cred = S.config_path()
    assert json.loads(cred.read_text())["token"]
    assert oct(cred.stat().st_mode & 0o777) == "0o600"

    merged = S.sync()
    assert merged["exercises"]["FND-01/01-a"]["status"] == "passed"
    # Sync never uploads anything beyond results: no quiz run ids.
    assert "taken" not in json.dumps(merged)

    # A fresh machine (empty progress) pulls everything back down.
    (repo / "progress.json").unlink()
    S.sync()
    pulled = P.load()
    assert pulled["exercises"]["FND-01/01-a"]["status"] == "passed"
    assert pulled["quizzes"]["FND-01"]["best_score"] == 0.9


def test_sync_with_revoked_or_unknown_token(env):
    web, repo = env
    S.save_credentials(SERVER, "unused", "alice")
    with pytest.raises(S.SyncError, match="revoked or is invalid"):
        S.sync()


def test_sync_requires_login(env):
    with pytest.raises(S.SyncError, match="not logged in"):
        S.sync()


def test_refuses_plain_http_to_remote_host():
    with pytest.raises(S.SyncError, match="plain http"):
        S._normalize_server("http://example.com")
    assert S._normalize_server("http://localhost:8100/") == "http://localhost:8100"
    assert S._normalize_server("https://de.example.com/") == "https://de.example.com"


def test_payload_excludes_everything_but_results():
    p = S._payload({"exercises": {"A-01/x": {"status": "passed", "attempts": 1, "minutes": 2,
                                             "secret_path": "/home/me/code"}},
                    "quizzes": {"A-01": {"best_score": 1.0, "taken": ["run"]}}, "current": None})
    assert p["exercises"]["A-01/x"] == {"status": "passed", "attempts": 1, "minutes": 2}
    assert p["quizzes"]["A-01"] == {"best_score": 1.0}

import sqlite3

import pytest
from fastapi.testclient import TestClient

from de_server import app as app_mod
from conftest_server import csrf_of, device_login, web_login

EX = {"status": "passed", "attempts": 2, "minutes": 30,
      "first_started": "2026-01-01T10:00:00+00:00", "passed_at": "2026-01-01T11:00:00+00:00"}


def auth(tok):
    return {"Authorization": f"Bearer {tok}"}


def test_landing_when_logged_out(client):
    r = client.get("/")
    assert r.status_code == 200 and "Sign in with GitHub" in r.text
    assert "default-src 'none'" in r.headers["content-security-policy"]


def test_oauth_rejects_wrong_state(client):
    client.get("/auth/login")
    assert client.get("/auth/github/callback?code=ok&state=forged").status_code == 400
    assert client.get("/auth/github/callback?code=ok").status_code == 400


def test_oauth_github_failure(client):
    state = client.get("/auth/login").headers["location"].split("state=")[1]
    assert client.get(f"/auth/github/callback?code=bad&state={state}").status_code == 502


def test_login_next_is_not_an_open_redirect(client):
    client.get("/auth/login?next=//evil.example")
    state = client.get("/auth/login?next=//evil.example").headers["location"].split("state=")[1]
    r = client.get(f"/auth/github/callback?code=ok&state={state}")
    assert r.headers["location"] == "/"


def test_device_flow_end_to_end(client):
    web_login(client)
    tok = device_login(client)
    assert client.get("/api/me", headers=auth(tok)).json() == {"login": "alice"}


def test_device_code_is_single_use_and_pending_before_approval(client):
    code = client.post("/api/device/code", json={}).json()
    r = client.post("/api/device/token", json={"device_code": code["device_code"]})
    assert r.json() == {"error": "authorization_pending"}
    web_login(client)
    client.post("/device", data={"user_code": code["user_code"], "csrf": csrf_of(client)})
    assert client.post("/api/device/token", json={"device_code": code["device_code"]}).status_code == 200
    again = client.post("/api/device/token", json={"device_code": code["device_code"]})
    assert again.status_code == 400 and again.json()["error"] == "invalid_grant"


def test_unknown_device_code(client):
    r = client.post("/api/device/token", json={"device_code": "x" * 43})
    assert r.json()["error"] == "invalid_grant"


def test_approve_needs_login_and_csrf(client):
    code = client.post("/api/device/code", json={}).json()
    assert client.post("/device", data={"user_code": code["user_code"], "csrf": "x"}).status_code == 401
    web_login(client)
    csrf_of(client)
    assert client.post("/device", data={"user_code": code["user_code"], "csrf": "wrong"}).status_code == 403


def test_approve_wrong_code_is_rate_limited(client):
    web_login(client)
    csrf = csrf_of(client)
    codes = [client.post("/device", data={"user_code": "AAAA-AAAA", "csrf": csrf}).status_code
             for _ in range(12)]
    assert codes[:10] == [400] * 10 and codes[10:] == [429, 429]


def test_slow_down_when_polling_too_fast(settings, gh, monkeypatch):
    monkeypatch.setattr(app_mod, "POLL_INTERVAL_S", 5)
    c = TestClient(app_mod.create_app(settings, gh), follow_redirects=False)
    code = c.post("/api/device/code", json={}).json()
    first = c.post("/api/device/token", json={"device_code": code["device_code"]})
    second = c.post("/api/device/token", json={"device_code": code["device_code"]})
    assert first.json()["error"] == "authorization_pending"
    assert second.json()["error"] == "slow_down"


def test_api_requires_valid_token(client):
    assert client.get("/api/progress").status_code == 401
    assert client.get("/api/progress", headers=auth("nope")).status_code == 401


def test_tokens_are_stored_hashed(client, settings):
    web_login(client)
    tok = device_login(client)
    rows = sqlite3.connect(settings.db_path).execute("SELECT token_hash FROM tokens").fetchall()
    assert rows and all(tok not in r[0] and len(r[0]) == 64 for r in rows)


def test_sync_merge_is_idempotent_and_monotonic(client):
    web_login(client)
    h = auth(device_login(client))
    body = {"exercises": {"FND-01/01-a": EX}, "quizzes": {"FND-01": {"best_score": 0.8}}}
    first = client.post("/api/sync", json=body, headers=h).json()
    assert client.post("/api/sync", json=body, headers=h).json()["exercises"] == first["exercises"]
    # Another machine with older/worse data cannot regress anything.
    stale = {"exercises": {"FND-01/01-a": {"status": "failed", "attempts": 1, "minutes": 5,
                                           "first_started": "2026-01-02T00:00:00+00:00"}},
             "quizzes": {"FND-01": {"best_score": 0.5}}}
    merged = client.post("/api/sync", json=stale, headers=h).json()
    e = merged["exercises"]["FND-01/01-a"]
    assert e["status"] == "passed" and e["attempts"] == 2 and e["minutes"] == 30
    assert e["first_started"] == EX["first_started"] and e["passed_at"] == EX["passed_at"]
    assert merged["quizzes"]["FND-01"]["best_score"] == 0.8


def test_users_are_isolated(client, settings, gh):
    web_login(client)
    h1 = auth(device_login(client))
    client.post("/api/sync", json={"exercises": {"FND-01/01-a": EX}}, headers=h1)
    gh.user = {"github_id": 2, "login": "bob", "avatar_url": ""}
    c2 = TestClient(app_mod.create_app(settings, gh), follow_redirects=False)
    web_login(c2)
    h2 = auth(device_login(c2))
    assert c2.get("/api/progress", headers=h2).json()["exercises"] == {}


@pytest.mark.parametrize("bad", [
    {"exercises": {"../etc/passwd": EX}},
    {"exercises": {"FND-01/01-a": {**EX, "status": "hacked"}}},
    {"exercises": {"FND-01/01-a": {**EX, "attempts": -1}}},
    {"exercises": {"FND-01/01-a": {**EX, "passed_at": "not a date"}}},
    {"quizzes": {"FND-01": {"best_score": 7}}},
    {"current": "bad id with spaces"},
])
def test_sync_rejects_invalid_payload(client, bad):
    web_login(client)
    h = auth(device_login(client))
    assert client.post("/api/sync", json=bad, headers=h).status_code == 422


def test_revoke_from_dashboard_and_api_logout(client):
    web_login(client)
    tok = device_login(client)
    page = client.get("/")
    assert page.status_code == 200 and "Connected CLIs" in page.text
    csrf = page.text.split('name="csrf" value="')[1].split('"')[0]
    assert client.post("/tokens/1/revoke", data={"csrf": csrf}).status_code == 303
    assert client.get("/api/me", headers=auth(tok)).status_code == 401
    tok2 = device_login(client)
    assert client.post("/api/logout", headers=auth(tok2)).status_code == 200
    assert client.get("/api/me", headers=auth(tok2)).status_code == 401


def test_dashboard_escapes_untrusted_values(client, gh, settings):
    gh.user = {"github_id": 9, "login": "<script>alert(1)</script>", "avatar_url": ""}
    web_login(client)
    html = client.get("/").text
    assert "<script>alert(1)</script>" not in html and "&lt;script&gt;" in html


def test_oversized_body_rejected(client):
    r = client.post("/api/device/code", content=b"x" * 10, headers={"content-length": "9999999"})
    assert r.status_code == 413

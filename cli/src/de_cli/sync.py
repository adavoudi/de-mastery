"""``de login`` / ``de sync``: device-code login and progress sync with the progress server.

The server is only a hub for state. The CLI always connects out; the server never runs
anything on the learner's machine, and only results (ids, status, counters) are uploaded,
never source code.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from de_cli import progress as progress_mod

CONFIG_ENV = "DE_CONFIG_DIR"
SERVER_ENV = "DE_SERVER_URL"
TIMEOUT_S = 15


class SyncError(Exception):
    pass


def config_path() -> Path:
    base = os.environ.get(CONFIG_ENV)
    d = Path(base) if base else Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "de"
    return d / "credentials.json"


def load_credentials() -> dict:
    p = config_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def save_credentials(server: str, token: str, login: str) -> None:
    p = config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)   # token: owner-only
    with os.fdopen(fd, "w") as f:
        json.dump({"server": server, "token": token, "login": login}, f)
    os.replace(tmp, p)


def clear_credentials() -> None:
    config_path().unlink(missing_ok=True)


def _normalize_server(url: str) -> str:
    url = url.strip().rstrip("/")
    if not url.startswith(("https://", "http://")):
        raise SyncError("server URL must start with https:// (or http:// for localhost)")
    host = url.split("://", 1)[1].split("/", 1)[0].split(":")[0]
    if url.startswith("http://") and host not in ("localhost", "127.0.0.1", "::1"):
        raise SyncError("refusing plain http:// to a remote server: your token would be "
                        "sent unencrypted")
    return url


def resolve_server(explicit: str | None) -> str:
    url = explicit or os.environ.get(SERVER_ENV) or load_credentials().get("server")
    if not url:
        raise SyncError(f"no server configured: pass --server URL or set {SERVER_ENV}")
    return _normalize_server(url)


def _request(method: str, url: str, body: dict | None = None,
             token: str | None = None) -> tuple[int, dict]:
    """One JSON HTTP call. Returns (status, json). Tests replace this function."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "User-Agent": "de-cli"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read() or b"{}")
        except ValueError:
            payload = {}
        return e.code, payload
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise SyncError(f"cannot reach {url}: {e}") from e


def login(server: str, say=print, sleep=time.sleep) -> str:
    """Run the device-code flow; stores the token and returns the GitHub login."""
    status, d = _request("POST", f"{server}/api/device/code", {})
    if status != 200:
        raise SyncError(f"server refused the login request (HTTP {status})")
    say(f"\nOpen  {d['verification_uri_complete']}")
    say(f"and approve this code if it matches:  {d['user_code']}\n")
    say("Waiting for approval (Ctrl+C to cancel)...")
    deadline = time.monotonic() + d["expires_in"]
    interval = d.get("interval", 5)
    while time.monotonic() < deadline:
        sleep(interval)
        status, t = _request("POST", f"{server}/api/device/token", {"device_code": d["device_code"]})
        if status == 200:
            save_credentials(server, t["access_token"], t["login"])
            return t["login"]
        err = t.get("error")
        if err == "authorization_pending":
            continue
        if err == "slow_down":
            interval += 5
            continue
        raise SyncError("login failed: " + {"expired_token": "the code expired",
                                            "invalid_grant": "the code is not valid"}.get(err, str(err)))
    raise SyncError("login timed out; run `de login` again")


def _payload(data: dict) -> dict:
    """Only what the server needs: no code, no run ids, no file paths."""
    return {
        "version": 1,
        "exercises": {
            k: {f: v[f] for f in ("status", "attempts", "minutes", "first_started", "passed_at")
                if f in v}
            for k, v in data.get("exercises", {}).items()
        },
        "quizzes": {k: {"best_score": v.get("best_score", 0.0)}
                    for k, v in data.get("quizzes", {}).items()},
        "current": data.get("current"),
    }


def sync(root: Path | None = None) -> dict:
    """Push local progress, then store the merged result locally. Returns the merged state."""
    creds = load_credentials()
    if not creds.get("token"):
        raise SyncError("not logged in: run `de login` first")
    server = _normalize_server(creds["server"])
    local = progress_mod.load(root)
    status, merged = _request("POST", f"{server}/api/sync", _payload(local), creds["token"])
    if status == 401:
        raise SyncError("token was revoked or is invalid: run `de login` again")
    if status != 200:
        raise SyncError(f"sync failed (HTTP {status}): {merged.get('detail', '')}")
    # Pull: the server merge is a superset of local, so adopt it (keeping local-only fields).
    local["exercises"] = {**local.get("exercises", {}), **merged["exercises"]}
    for k, v in merged["quizzes"].items():
        q = local["quizzes"].setdefault(k, {"best_score": 0.0, "taken": []})
        q["best_score"] = max(q.get("best_score", 0.0), v["best_score"])
    if not local.get("current"):
        local["current"] = merged.get("current")
    progress_mod.save(local, root)
    return merged


def logout() -> bool:
    """Revoke the token on the server (best effort) and delete it locally."""
    creds = load_credentials()
    if not creds.get("token"):
        return False
    try:
        _request("POST", f"{_normalize_server(creds['server'])}/api/logout", {}, creds["token"])
    except SyncError:
        pass
    clear_credentials()
    return True

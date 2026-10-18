"""FastAPI app: GitHub login for the web UI, device-code login for the CLI, sync API, dashboard."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

from de_server import catalog as catalog_mod
from de_server import db as db_mod
from de_server import merge as merge_mod
from de_server.config import Settings
from de_server.github import GitHubOAuth

HERE = Path(__file__).parent
DEVICE_TTL_S = 600
POLL_INTERVAL_S = 5
# No 0/O/1/I/L: the code is typed by a human.
USER_CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
MAX_BODY = 2_000_000


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _new_user_code() -> str:
    raw = "".join(secrets.choice(USER_CODE_ALPHABET) for _ in range(8))
    return f"{raw[:4]}-{raw[4:]}"


def _norm_user_code(s: str) -> str:
    s = "".join(ch for ch in s.upper() if ch.isalnum())
    return f"{s[:4]}-{s[4:]}" if len(s) == 8 else ""


class RateLimiter:
    """In-memory sliding window; enough for a single-process hobby server."""

    def __init__(self, limit: int, window_s: float):
        self.limit, self.window_s = limit, window_s
        self.hits: dict[str, list[float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        recent = [t for t in self.hits.get(key, []) if now - t < self.window_s]
        if len(recent) >= self.limit:
            self.hits[key] = recent
            return False
        recent.append(now)
        self.hits[key] = recent
        return True


class DeviceTokenIn(BaseModel):
    device_code: str = Field(min_length=20, max_length=200)


class DeviceCodeIn(BaseModel):
    label: str = Field("", max_length=60)


def create_app(settings: Settings, github: GitHubOAuth | None = None) -> FastAPI:
    db_mod.init(settings.db_path)
    github = github or GitHubOAuth(settings.github_client_id, settings.github_client_secret,
                                   f"{settings.base_url}/auth/github/callback")
    catalog = catalog_mod.load(settings.repo_root)
    templates = Jinja2Templates(directory=str(HERE / "templates"))
    code_limiter = RateLimiter(10, 60)       # device-code creation per IP
    approve_limiter = RateLimiter(10, 600)   # wrong user-code guesses per user
    sync_limiter = RateLimiter(60, 60)       # per token

    app = FastAPI(title="DE Mastery progress server", docs_url=None, redoc_url=None,
                  openapi_url=None)
    app.add_middleware(SessionMiddleware, secret_key=settings.session_secret,
                       session_cookie="de_session", max_age=14 * 24 * 3600,
                       same_site="lax", https_only=settings.https)
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        length = request.headers.get("content-length")
        if length and length.isdigit() and int(length) > MAX_BODY:
            return JSONResponse({"error": "payload too large"}, status_code=413)
        resp = await call_next(request)
        resp.headers["Content-Security-Policy"] = (
            "default-src 'none'; style-src 'self'; img-src 'self' https://avatars.githubusercontent.com; "
            "form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Referrer-Policy"] = "same-origin"
        resp.headers["Cache-Control"] = "no-store"
        if settings.https:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000"
        return resp

    # -- helpers -----------------------------------------------------------
    def get_conn():
        conn = db_mod.connect(settings.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def session_user(request: Request, conn: sqlite3.Connection) -> sqlite3.Row | None:
        uid = request.session.get("uid")
        if not uid:
            return None
        return conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()

    def csrf_token(request: Request) -> str:
        tok = request.session.get("csrf")
        if not tok:
            tok = secrets.token_urlsafe(32)
            request.session["csrf"] = tok
        return tok

    def check_csrf(request: Request, token: str) -> None:
        good = request.session.get("csrf", "")
        if not good or not hmac.compare_digest(good, token):
            raise HTTPException(403, "bad CSRF token")

    def api_user(request: Request, conn: sqlite3.Connection = Depends(get_conn)):
        auth = request.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise HTTPException(401, "missing bearer token")
        row = conn.execute(
            "SELECT t.id AS token_id, u.* FROM tokens t JOIN users u ON u.id=t.user_id "
            "WHERE t.token_hash=? AND t.revoked=0", (_sha(auth[7:].strip()),)).fetchone()
        if row is None:
            raise HTTPException(401, "invalid or revoked token")
        if not sync_limiter.allow(str(row["token_id"])):
            raise HTTPException(429, "too many requests")
        conn.execute("UPDATE tokens SET last_used=? WHERE id=?", (_now_iso(), row["token_id"]))
        conn.commit()
        return row

    def safe_next(value: str | None) -> str:
        """Only same-site relative paths; blocks open redirects like //evil.com."""
        if value and value.startswith("/") and not value.startswith("//") and "\\" not in value:
            return value
        return "/"

    # -- web: login --------------------------------------------------------
    @app.get("/auth/login")
    def login(request: Request, next: str = "/"):
        state = secrets.token_urlsafe(24)
        request.session["oauth_state"] = state
        request.session["next"] = safe_next(next)
        return RedirectResponse(github.authorize_url(state), status_code=302)

    @app.get("/auth/github/callback")
    def callback(request: Request, code: str = "", state: str = "",
                 conn: sqlite3.Connection = Depends(get_conn)):
        expected = request.session.pop("oauth_state", "")
        if not expected or not hmac.compare_digest(expected, state) or not code:
            raise HTTPException(400, "invalid OAuth state")
        try:
            gh = github.fetch_user(code)
        except Exception:
            raise HTTPException(502, "GitHub login failed")
        conn.execute(
            "INSERT INTO users(github_id, login, avatar_url, created_at) VALUES (?,?,?,?) "
            "ON CONFLICT(github_id) DO UPDATE SET login=excluded.login, avatar_url=excluded.avatar_url",
            (gh["github_id"], gh["login"], gh["avatar_url"], _now_iso()))
        conn.commit()
        user = conn.execute("SELECT id FROM users WHERE github_id=?", (gh["github_id"],)).fetchone()
        nxt = safe_next(request.session.pop("next", "/"))
        request.session.clear()                      # new session on login
        request.session["uid"] = user["id"]
        return RedirectResponse(nxt, status_code=302)

    @app.post("/auth/logout")
    def logout(request: Request, csrf: str = Form("")):
        check_csrf(request, csrf)
        request.session.clear()
        return RedirectResponse("/", status_code=303)

    # -- web: device approval ---------------------------------------------
    @app.get("/device", response_class=HTMLResponse)
    def device_page(request: Request, code: str = "",
                    conn: sqlite3.Connection = Depends(get_conn)):
        user = session_user(request, conn)
        if user is None:
            nxt = "/device" + (f"?code={quote(code[:9])}" if code else "")
            return RedirectResponse(f"/auth/login?next={quote(nxt)}", status_code=302)
        return templates.TemplateResponse(request, "device.html", {
            "user": user, "csrf": csrf_token(request), "code": _norm_user_code(code) or code[:9],
            "message": None, "ok": False})

    @app.post("/device", response_class=HTMLResponse)
    def device_approve(request: Request, user_code: str = Form(""), csrf: str = Form(""),
                       conn: sqlite3.Connection = Depends(get_conn)):
        user = session_user(request, conn)
        if user is None:
            raise HTTPException(401, "login required")
        check_csrf(request, csrf)

        def page(message: str, ok: bool, status: int = 200):
            return templates.TemplateResponse(request, "device.html", {
                "user": user, "csrf": csrf_token(request), "code": "", "message": message,
                "ok": ok}, status_code=status)

        if not approve_limiter.allow(str(user["id"])):
            return page("Too many attempts. Wait a few minutes and try again.", False, 429)
        code = _norm_user_code(user_code)
        row = conn.execute(
            "SELECT device_hash FROM device_codes WHERE user_code=? AND user_id IS NULL "
            "AND consumed=0 AND expires_at>?", (code, time.time())).fetchone() if code else None
        if row is None:
            return page("That code is not valid or has expired. Run `de login` again.", False, 400)
        conn.execute("UPDATE device_codes SET user_id=? WHERE device_hash=?",
                     (user["id"], row["device_hash"]))
        conn.commit()
        return page("Approved. You can return to your terminal.", True)

    # -- API: device flow --------------------------------------------------
    @app.post("/api/device/code")
    def device_code(body: DeviceCodeIn, request: Request,
                    conn: sqlite3.Connection = Depends(get_conn)):
        if not code_limiter.allow(request.client.host if request.client else "?"):
            raise HTTPException(429, "too many requests")
        conn.execute("DELETE FROM device_codes WHERE expires_at<?", (time.time() - 3600,))
        device = secrets.token_urlsafe(32)
        for _ in range(5):
            user_code = _new_user_code()
            try:
                conn.execute("INSERT INTO device_codes(device_hash, user_code, expires_at) "
                             "VALUES (?,?,?)", (_sha(device), user_code, time.time() + DEVICE_TTL_S))
                break
            except sqlite3.IntegrityError:
                continue
        else:
            raise HTTPException(503, "try again")
        conn.commit()
        return {"device_code": device, "user_code": user_code,
                "verification_uri": f"{settings.base_url}/device",
                "verification_uri_complete": f"{settings.base_url}/device?code={user_code}",
                "expires_in": DEVICE_TTL_S, "interval": POLL_INTERVAL_S}

    @app.post("/api/device/token")
    def device_token(body: DeviceTokenIn, conn: sqlite3.Connection = Depends(get_conn)):
        h = _sha(body.device_code)
        row = conn.execute("SELECT * FROM device_codes WHERE device_hash=?", (h,)).fetchone()
        if row is None or row["consumed"]:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)
        now = time.time()
        if row["expires_at"] < now:
            return JSONResponse({"error": "expired_token"}, status_code=400)
        if now - row["last_poll"] < POLL_INTERVAL_S - 0.5:
            conn.execute("UPDATE device_codes SET last_poll=? WHERE device_hash=?", (now, h))
            conn.commit()
            return JSONResponse({"error": "slow_down"}, status_code=400)
        conn.execute("UPDATE device_codes SET last_poll=? WHERE device_hash=?", (now, h))
        if row["user_id"] is None:
            conn.commit()
            return JSONResponse({"error": "authorization_pending"}, status_code=400)
        token = secrets.token_urlsafe(32)
        conn.execute("UPDATE device_codes SET consumed=1 WHERE device_hash=?", (h,))
        conn.execute("INSERT INTO tokens(token_hash, user_id, created_at) VALUES (?,?,?)",
                     (_sha(token), row["user_id"], _now_iso()))
        conn.commit()
        user = conn.execute("SELECT login FROM users WHERE id=?", (row["user_id"],)).fetchone()
        return {"access_token": token, "token_type": "bearer", "login": user["login"]}

    # -- API: sync ---------------------------------------------------------
    @app.get("/api/me")
    def me(user=Depends(api_user)):
        return {"login": user["login"]}

    @app.post("/api/sync")
    def sync(payload: merge_mod.ProgressIn, user=Depends(api_user),
             conn: sqlite3.Connection = Depends(get_conn)):
        merge_mod.merge_into(conn, user["id"], payload)
        conn.commit()
        return merge_mod.load_state(conn, user["id"])

    @app.get("/api/progress")
    def progress(user=Depends(api_user), conn: sqlite3.Connection = Depends(get_conn)):
        return merge_mod.load_state(conn, user["id"])

    @app.post("/api/logout")
    def api_logout(user=Depends(api_user), conn: sqlite3.Connection = Depends(get_conn)):
        conn.execute("UPDATE tokens SET revoked=1 WHERE id=?", (user["token_id"],))
        conn.commit()
        return {"ok": True}

    # -- web: dashboard ----------------------------------------------------
    @app.get("/", response_class=HTMLResponse)
    def home(request: Request, conn: sqlite3.Connection = Depends(get_conn)):
        user = session_user(request, conn)
        if user is None:
            return templates.TemplateResponse(request, "landing.html", {})
        state = merge_mod.load_state(conn, user["id"])
        ex = state["exercises"]
        rows = []
        for track, ids in sorted(catalog.items()):
            passed = sum(1 for i in ids if ex.get(i, {}).get("status") == "passed")
            rows.append({"track": track, "passed": passed, "total": len(ids),
                         "pct": round(100 * passed / len(ids)) if ids else 0})
        known = {i for ids in catalog.values() for i in ids}
        extra = sum(1 for i, e in ex.items() if e["status"] == "passed" and i not in known)
        tokens = conn.execute(
            "SELECT id, created_at, last_used FROM tokens WHERE user_id=? AND revoked=0 "
            "ORDER BY id DESC", (user["id"],)).fetchall()
        return templates.TemplateResponse(request, "dashboard.html", {
            "user": user, "rows": rows, "extra_passed": extra, "state": state,
            "total_passed": sum(1 for e in ex.values() if e["status"] == "passed"),
            "hours": round(sum(e["minutes"] for e in ex.values()) / 60, 1),
            "tokens": tokens, "csrf": csrf_token(request)})

    @app.post("/tokens/{token_id}/revoke")
    def revoke(token_id: int, request: Request, csrf: str = Form(""),
               conn: sqlite3.Connection = Depends(get_conn)):
        user = session_user(request, conn)
        if user is None:
            raise HTTPException(401, "login required")
        check_csrf(request, csrf)
        conn.execute("UPDATE tokens SET revoked=1 WHERE id=? AND user_id=?", (token_id, user["id"]))
        conn.commit()
        return RedirectResponse("/", status_code=303)

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app

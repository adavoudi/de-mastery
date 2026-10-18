"""FastAPI app factory for the course website. No accounts, no database: it only reads the repo."""

from __future__ import annotations

import hashlib
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from de_web import site as site_mod
from de_web.library import Library

HERE = Path(__file__).parent
DEFAULT_SOURCE_URL = "https://github.com/adavoudi/de-mastery"


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def create_app(repo_root: Path | None = None, source_url: str = DEFAULT_SOURCE_URL) -> FastAPI:
    library = Library(repo_root or default_repo_root(), source_url)
    templates = Jinja2Templates(directory=str(HERE / "templates"))

    def asset(name: str) -> str:
        """``/static/<name>?v=<hash>``: long-cached, but a changed file gets a new URL."""
        try:
            digest = hashlib.sha256((HERE / "static" / name).read_bytes()).hexdigest()[:10]
        except OSError:
            digest = "0"
        return f"/static/{name}?v={digest}"

    templates.env.globals["asset"] = asset

    app = FastAPI(title="DE Mastery", docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        resp = await call_next(request)
        resp.headers["Content-Security-Policy"] = (
            "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; "
            "img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Referrer-Policy"] = "same-origin"
        if request.url.path.startswith("/static/"):
            resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon():
        return FileResponse(HERE / "static" / "brand" / "favicon.ico", media_type="image/x-icon",
                            headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    site_mod.register(app, library, templates)
    return app

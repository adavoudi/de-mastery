"""``python -m de_web``: serve the course site on localhost (or behind a reverse proxy)."""

import argparse
from pathlib import Path

import uvicorn

from de_web.app import create_app


def main() -> None:
    ap = argparse.ArgumentParser(description="Serve the DE Mastery course website.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--repo", type=Path, default=None, help="course repo to serve (default: this checkout)")
    a = ap.parse_args()
    uvicorn.run(create_app(a.repo), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()

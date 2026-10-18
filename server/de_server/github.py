"""GitHub OAuth (web application flow). Behind a small class so tests can inject a fake."""

from __future__ import annotations

from urllib.parse import urlencode

import httpx


class GitHubOAuth:
    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def authorize_url(self, state: str) -> str:
        q = urlencode({"client_id": self.client_id, "redirect_uri": self.redirect_uri,
                       "state": state, "scope": "read:user", "allow_signup": "true"})
        return f"https://github.com/login/oauth/authorize?{q}"

    def fetch_user(self, code: str) -> dict:
        """Exchange ``code`` for a token, then read the profile. Raises on any failure."""
        with httpx.Client(timeout=10) as c:
            r = c.post("https://github.com/login/oauth/access_token",
                       headers={"Accept": "application/json"},
                       data={"client_id": self.client_id, "client_secret": self.client_secret,
                             "code": code, "redirect_uri": self.redirect_uri})
            r.raise_for_status()
            token = r.json().get("access_token")
            if not token:
                raise ValueError("GitHub did not return an access token")
            u = c.get("https://api.github.com/user",
                      headers={"Authorization": f"Bearer {token}",
                               "Accept": "application/vnd.github+json"})
            u.raise_for_status()
            d = u.json()
        return {"github_id": int(d["id"]), "login": str(d["login"]),
                "avatar_url": str(d.get("avatar_url") or "")}

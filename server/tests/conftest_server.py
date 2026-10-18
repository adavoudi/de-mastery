"""Test doubles shared by server and CLI tests."""


class FakeGitHub:
    def __init__(self):
        self.user = {"github_id": 1, "login": "alice", "avatar_url": ""}

    def authorize_url(self, state):
        return f"https://github.test/authorize?state={state}"

    def fetch_user(self, code):
        if code == "bad":
            raise ValueError("denied")
        return dict(self.user)


def web_login(client, gh_state_client=None):
    """Complete the GitHub web login on ``client`` (cookie jar keeps the session)."""
    r = client.get("/auth/login")
    state = r.headers["location"].split("state=")[1]
    r = client.get(f"/auth/github/callback?code=ok&state={state}")
    assert r.status_code == 302
    return r


def csrf_of(client):
    page = client.get("/device")
    assert page.status_code == 200
    return page.text.split('name="csrf" value="')[1].split('"')[0]


def device_login(client, label=""):
    """Run the full device flow; returns the bearer token. ``client`` must be web-logged-in."""
    code = client.post("/api/device/code", json={}).json()
    r = client.post("/device", data={"user_code": code["user_code"], "csrf": csrf_of(client)})
    assert r.status_code == 200 and "Approved" in r.text
    t = client.post("/api/device/token", json={"device_code": code["device_code"]})
    assert t.status_code == 200, t.text
    return t.json()["access_token"]

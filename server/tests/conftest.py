import pytest
from fastapi.testclient import TestClient

from de_server import app as app_mod
from de_server.config import Settings
from conftest_server import FakeGitHub


@pytest.fixture
def settings(tmp_path):
    return Settings(base_url="http://localhost:8100", session_secret="x" * 40,
                    github_client_id="id", github_client_secret="secret",
                    db_path=tmp_path / "t.sqlite3")


@pytest.fixture
def gh():
    return FakeGitHub()


@pytest.fixture
def client(settings, gh, monkeypatch):
    monkeypatch.setattr(app_mod, "POLL_INTERVAL_S", 0)
    return TestClient(app_mod.create_app(settings, gh), follow_redirects=False)

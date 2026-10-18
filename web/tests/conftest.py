import pytest
from fastapi.testclient import TestClient

from de_web.app import create_app


@pytest.fixture(scope="session")
def client():
    return TestClient(create_app())

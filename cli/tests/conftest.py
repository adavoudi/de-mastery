import os
from pathlib import Path

import pytest

FIXTURE_CONTENT = Path(__file__).parent / "fixtures" / "content"


@pytest.fixture
def fixture_content_root():
    return FIXTURE_CONTENT


@pytest.fixture
def dedup_exercise():
    from de_cli.content import find_exercise
    ex = find_exercise("ZZZ-01/01-dedup-orders", FIXTURE_CONTENT)
    assert ex is not None, "fixture exercise not found"
    return ex


@pytest.fixture
def temp_repo(tmp_path, monkeypatch):
    """A throwaway repo root with pyproject.toml + learner.yaml so progress/plan
    write into tmp_path instead of the real repo."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='t'\nversion='0'\n")
    monkeypatch.chdir(tmp_path)
    return tmp_path

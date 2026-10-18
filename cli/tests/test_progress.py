"""progress.json read/write and state transitions."""

import json

from de_cli import progress as P


def _root(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='t'\nversion='0'\n")
    return tmp_path


def test_load_default_when_missing(tmp_path):
    root = _root(tmp_path)
    data = P.load(root)
    assert data["version"] == 1
    assert data["exercises"] == {}
    assert data["current"] is None


def test_record_start_and_attempts(tmp_path):
    root = _root(tmp_path)
    P.record_start("ZZZ-01/01-x", root)
    data = P.load(root)
    assert data["current"] == "ZZZ-01/01-x"
    assert data["exercises"]["ZZZ-01/01-x"]["status"] == "started"

    P.record_attempt("ZZZ-01/01-x", passed=False, minutes=5, root=root)
    P.record_attempt("ZZZ-01/01-x", passed=False, minutes=5, root=root)
    data = P.load(root)
    ex = data["exercises"]["ZZZ-01/01-x"]
    assert ex["attempts"] == 2
    assert ex["minutes"] == 10
    assert ex["status"] == "failed"

    P.record_attempt("ZZZ-01/01-x", passed=True, minutes=3, root=root)
    data = P.load(root)
    ex = data["exercises"]["ZZZ-01/01-x"]
    assert ex["status"] == "passed"
    assert ex["attempts"] == 3
    assert "passed_at" in ex


def test_record_quiz_keeps_best(tmp_path):
    root = _root(tmp_path)
    P.record_quiz("ZZZ-01", 0.5, "run1", root)
    P.record_quiz("ZZZ-01", 0.9, "run2", root)
    P.record_quiz("ZZZ-01", 0.7, "run3", root)
    data = P.load(root)
    q = data["quizzes"]["ZZZ-01"]
    assert q["best_score"] == 0.9
    assert q["taken"] == ["run1", "run2", "run3"]


def test_progress_file_is_valid_json(tmp_path):
    root = _root(tmp_path)
    P.record_start("A/01", root)
    raw = (root / "progress.json").read_text()
    parsed = json.loads(raw)
    assert parsed["current"] == "A/01"

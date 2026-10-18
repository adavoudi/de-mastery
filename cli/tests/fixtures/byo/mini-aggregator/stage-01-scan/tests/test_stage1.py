"""Stage 1 tests: scan returns rows as a list of dicts."""


def test_scan_returns_all_rows(submission):
    rows = [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}]
    out = submission.scan(rows)
    assert isinstance(out, list)
    assert len(out) == 3
    assert {r["k"] for r in out} == {"a", "b"}
    assert sorted(r["v"] for r in out) == [1, 2, 3]


def test_scan_empty(submission):
    assert submission.scan([]) == []

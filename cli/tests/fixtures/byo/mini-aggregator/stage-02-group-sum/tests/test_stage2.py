"""Stage 2 tests: group_sum groups by key and sums val."""


def _as_map(out, key, val):
    return {r[key]: r[val] for r in out}


def test_group_sum_basic(submission):
    rows = [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}]
    out = submission.group_sum(rows, "k", "v")
    assert isinstance(out, list)
    assert _as_map(out, "k", "v") == {"a": 4, "b": 2}


def test_group_sum_single_group(submission):
    rows = [{"k": "x", "v": 5}, {"k": "x", "v": 7}]
    out = submission.group_sum(rows, "k", "v")
    assert _as_map(out, "k", "v") == {"x": 12}


def test_group_sum_empty(submission):
    assert submission.group_sum([], "k", "v") == []

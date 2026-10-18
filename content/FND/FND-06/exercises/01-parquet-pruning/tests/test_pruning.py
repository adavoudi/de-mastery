"""Tests for FND-06/01-parquet-pruning.

The tests WRITE controlled Parquet files (known row-group boundaries), so expected
pruning is known. DuckDB is the oracle for which row groups actually contain matching
rows; the key invariant is SOUNDNESS: pruning never drops a matching row.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

OPS = {
    ">": lambda x, v: x > v,
    ">=": lambda x, v: x >= v,
    "<": lambda x, v: x < v,
    "<=": lambda x, v: x <= v,
    "==": lambda x, v: x == v,
}


def _write(tmp_path, values, row_group_size, write_statistics=True, name="t.parquet"):
    table = pa.table({"x": pa.array(values, type=pa.int64())})
    path = tmp_path / name
    pq.write_table(table, path, row_group_size=row_group_size,
                   write_statistics=write_statistics)
    return path


def _groups_actually_containing_matches(values, row_group_size, op, value):
    """Independent oracle: which row-group indices really hold a matching row."""
    pred = OPS[op]
    groups = []
    for gi, start in enumerate(range(0, len(values), row_group_size)):
        chunk = values[start:start + row_group_size]
        if any(pred(x, value) for x in chunk):
            groups.append(gi)
    return set(groups)


# ---------------------------------------------------------------------------
# footer_stats
# ---------------------------------------------------------------------------
def test_footer_stats_reports_per_group_min_max(submission, tmp_path):
    values = list(range(0, 100))         # sorted 0..99
    path = _write(tmp_path, values, row_group_size=25)   # 4 row groups
    stats = submission.footer_stats(path, "x")
    assert [s["row_group"] for s in stats] == [0, 1, 2, 3]
    assert [s["num_rows"] for s in stats] == [25, 25, 25, 25]
    assert stats[0]["min"] == 0 and stats[0]["max"] == 24
    assert stats[3]["min"] == 75 and stats[3]["max"] == 99


# ---------------------------------------------------------------------------
# Known pruning on sorted data
# ---------------------------------------------------------------------------
def test_known_pruning_on_sorted_data(submission, tmp_path):
    values = list(range(0, 100))
    path = _write(tmp_path, values, row_group_size=25)   # groups cover 0-24,25-49,50-74,75-99
    # x > 80 -> only the last group (75..99) can match.
    assert submission.row_groups_to_scan(path, "x", ">", 80) == [3]
    # x < 10 -> only the first group.
    assert submission.row_groups_to_scan(path, "x", "<", 10) == [0]
    # x == 60 -> only group 2 (50..74).
    assert submission.row_groups_to_scan(path, "x", "==", 60) == [2]
    # x >= 25 -> groups 1,2,3.
    assert submission.row_groups_to_scan(path, "x", ">=", 25) == [1, 2, 3]
    # Something that prunes at least one group proves pruning really happens.
    assert len(submission.row_groups_to_scan(path, "x", ">", 80)) < 4


def test_matches_duckdb_oracle_on_which_groups_have_matches(submission, tmp_path,
                                                            duckdb_con):
    values = list(range(0, 100))
    rgs = 20
    path = _write(tmp_path, values, row_group_size=rgs)
    for op in OPS:
        for value in (5, 25, 50, 99, 100, -1):
            scan = set(submission.row_groups_to_scan(path, "x", op, value))
            actual = _groups_actually_containing_matches(values, rgs, op, value)
            # SOUND: every group that truly has a match must be scanned.
            assert actual <= scan, (
                f"op={op} value={value}: dropped groups {actual - scan}")


# ---------------------------------------------------------------------------
# Missing statistics -> never prune
# ---------------------------------------------------------------------------
def test_no_stats_means_no_pruning(submission, tmp_path):
    values = list(range(0, 100))
    path = _write(tmp_path, values, row_group_size=25, write_statistics=False,
                  name="nostats.parquet")
    # Without stats, a sound engine must scan every group for any predicate.
    for op in OPS:
        scan = submission.row_groups_to_scan(path, "x", op, 50)
        assert scan == [0, 1, 2, 3], f"op={op} pruned despite missing stats: {scan}"


# ---------------------------------------------------------------------------
# Hypothesis: soundness over random data / predicates / group sizes
# ---------------------------------------------------------------------------
@settings(max_examples=150, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(values=st.lists(st.integers(min_value=-30, max_value=30), min_size=1,
                       max_size=200),
       rgs=st.integers(min_value=1, max_value=40),
       op=st.sampled_from(list(OPS.keys())),
       value=st.integers(min_value=-35, max_value=35))
def test_pruning_is_sound(submission, tmp_path, values, rgs, op, value):
    path = _write(tmp_path, values, row_group_size=rgs, name="prop.parquet")
    scan = set(submission.row_groups_to_scan(path, "x", op, value))
    actual = _groups_actually_containing_matches(values, rgs, op, value)
    assert actual <= scan, (
        f"unsound: op={op} value={value} dropped groups {actual - scan}")

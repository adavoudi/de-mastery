"""Stage 3 — predicate pushdown using Parquet row-group statistics.

A Scan with a ``pushdown`` predicate must skip whole row groups whose min/max
statistics prove no row can match — and must report how many it skipped, like
Spark's ``PushedFilters`` / row-group pruning. Correctness is unchanged: results
still equal the DuckDB oracle.

The data is written SORTED by ``id`` with small row groups so each row group
covers a disjoint id range; a selective predicate can then skip most of them.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from de_testkit import assert_frames_equal


def _write_sorted(path, n=1000, rg=100):
    ids = list(range(n))  # already sorted -> disjoint row-group ranges
    t = pa.table({
        "id": pa.array(ids, pa.int64()),
        "amt": pa.array([float(i) for i in ids], pa.float64()),
    })
    pq.write_table(t, path, row_group_size=rg)
    return t


def _scan_with_stats(submission, p, pred, columns=None):
    scan = submission.new_scan(p, columns=columns, pushdown=pred)
    out = submission.execute(scan)
    return scan, out


def test_pushdown_skips_row_groups(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "s.parquet")
    _write_sorted(p, n=1000, rg=100)  # 10 row groups, ids 0..999
    assert pq.ParquetFile(p).num_row_groups == 10
    # id > 950 -> only the last row group (900..999) can match.
    pred = submission.Compare(submission.Col("id"), ">", submission.Lit(950))
    scan, out = _scan_with_stats(submission, p, pred)
    # Correctness: still filtered correctly (pushdown prunes, Filter not required
    # because pushdown here is exact at the boundary? No — assert >= via oracle).
    # The scan alone returns full row groups that survived pruning; wrap a Filter
    # so the final rows are exact, but the SKIP must have happened in the scan.
    exact = submission.execute(submission.Filter(
        submission.new_scan(p, pushdown=pred), pred))
    expected = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{p}') WHERE id > 950").arrow()
    assert_frames_equal(exact, expected)
    # The counter proves pruning happened (at least 8 of 10 skipped).
    assert scan.stats.row_groups_total == 10
    assert scan.stats.row_groups_skipped >= 8
    assert scan.stats.row_groups_read <= 2


def test_explain_reports_pushed_filters(submission, tmp_path):
    p = str(tmp_path / "s.parquet")
    _write_sorted(p)
    pred = submission.Compare(submission.Col("id"), "<", submission.Lit(50))
    scan = submission.new_scan(p, pushdown=pred)
    submission.execute(scan)
    text = submission.explain(scan)
    assert "PushedFilters" in text
    # Reports skipped row groups somewhere in the explain string.
    assert "skipped" in text.lower() or "rowGroups" in text


def test_pushdown_no_skip_when_all_match(submission, tmp_path):
    p = str(tmp_path / "s.parquet")
    _write_sorted(p, n=1000, rg=100)
    pred = submission.Compare(submission.Col("id"), ">=", submission.Lit(0))
    scan = submission.new_scan(p, pushdown=pred)
    submission.execute(scan)
    assert scan.stats.row_groups_skipped == 0
    assert scan.stats.row_groups_read == 10


def test_pushdown_correct_with_equality(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "s.parquet")
    _write_sorted(p, n=1000, rg=100)
    # id = 942 lives in the LAST row group (900..999). Every lower row group has
    # cmax < 942, so correct equality pruning must skip all 9 of them. A pruner
    # that only checks the lower bound (v < cmin) will fail to skip them.
    pred = submission.Compare(submission.Col("id"), "=", submission.Lit(942))
    scan = submission.new_scan(p, pushdown=pred)
    out = submission.execute(submission.Filter(scan, pred))
    expected = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{p}') WHERE id = 942").arrow()
    assert_frames_equal(out, expected)
    assert scan.stats.row_groups_skipped == 9
    assert scan.stats.row_groups_read == 1


@settings(max_examples=20, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(threshold=st.integers(min_value=0, max_value=999))
def test_pushdown_never_drops_matching_rows(submission, tmp_path, duckdb_con, threshold):
    """Soundness of pruning: a skipped row group must contain no matching row,
    i.e. the final (scan+filter) result still equals the oracle for every
    threshold, regardless of how aggressively row groups are skipped."""
    p = str(tmp_path / "prop.parquet")
    _write_sorted(p, n=1000, rg=100)
    pred = submission.Compare(submission.Col("id"), ">=", submission.Lit(threshold))
    scan = submission.new_scan(p, pushdown=pred)
    out = submission.execute(submission.Filter(scan, pred))
    expected = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{p}') WHERE id >= {threshold}").arrow()
    assert_frames_equal(out, expected)

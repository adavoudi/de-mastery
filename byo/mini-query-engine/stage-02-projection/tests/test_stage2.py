"""Stage 2 — projection and filter operators (Volcano/iterator model).

Build Project/Filter nodes over a Scan and compare with DuckDB (G6). A
Hypothesis property checks filter soundness: every surviving row satisfies the
predicate and no satisfying row is dropped.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from de_testkit import assert_frames_equal


def _write(path, n=250, rg=80):
    ids = list(range(n))
    cats = ["a", "b", "c", "d"]
    t = pa.table({
        "id": pa.array(ids, pa.int64()),
        "cat": pa.array([cats[i % 4] for i in ids]),
        "amt": pa.array([float(i % 50) for i in ids], pa.float64()),
    })
    pq.write_table(t, path, row_group_size=rg)
    return t


def test_filter_matches_duckdb(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Filter(
        submission.new_scan(p),
        submission.Compare(submission.Col("amt"), ">", submission.Lit(25.0)),
    )
    out = submission.execute(plan)
    expected = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{p}') WHERE amt > 25.0").arrow()
    assert_frames_equal(out, expected)


def test_project_then_filter(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Project(
        submission.Filter(
            submission.new_scan(p),
            submission.Compare(submission.Col("cat"), "=", submission.Lit("a")),
        ),
        columns=["id", "amt"],
    )
    out = submission.execute(plan)
    assert set(out.column_names) == {"id", "amt"}
    expected = duckdb_con.execute(
        f"SELECT id, amt FROM read_parquet('{p}') WHERE cat = 'a'").arrow()
    assert_frames_equal(out, expected)


def test_and_or_predicates(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "d.parquet")
    _write(p)
    pred = submission.And(
        submission.Compare(submission.Col("amt"), ">=", submission.Lit(10.0)),
        submission.Or(
            submission.Compare(submission.Col("cat"), "=", submission.Lit("a")),
            submission.Compare(submission.Col("cat"), "=", submission.Lit("b")),
        ),
    )
    out = submission.execute(submission.Filter(submission.new_scan(p), pred))
    expected = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{p}') "
        "WHERE amt >= 10.0 AND (cat = 'a' OR cat = 'b')").arrow()
    assert_frames_equal(out, expected)


@settings(max_examples=25, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(threshold=st.integers(min_value=0, max_value=49))
def test_filter_soundness_property(submission, tmp_path, duckdb_con, threshold):
    p = str(tmp_path / "prop.parquet")
    _write(p)
    plan = submission.Filter(
        submission.new_scan(p),
        submission.Compare(submission.Col("amt"), ">", submission.Lit(float(threshold))),
    )
    out = submission.execute(plan)
    # soundness: every row satisfies the predicate
    for v in out.column("amt").to_pylist():
        assert v > threshold
    # completeness: same count as the oracle
    exp = duckdb_con.execute(
        f"SELECT count(*) c FROM read_parquet('{p}') WHERE amt > {float(threshold)}"
    ).fetchone()[0]
    assert out.num_rows == exp

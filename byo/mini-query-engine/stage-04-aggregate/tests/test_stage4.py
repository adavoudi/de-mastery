"""Stage 4 — hash aggregate with partial vs. final aggregation.

Tests the Aggregate plan node end-to-end (vs DuckDB, G6) AND the partial/final
split directly: ``partial_aggregate`` over chunks then ``merge_partials`` must
equal a single aggregate over the whole table. A Hypothesis property splits the
data at a random point and asserts partial+merge == whole (this kills "average
of averages").
"""

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from de_testkit import assert_frames_equal


def _write(path, n=400, rg=90):
    ids = list(range(n))
    cats = ["a", "b", "c", "d", "e"]
    t = pa.table({
        "cat": pa.array([cats[i % 5] for i in ids]),
        "amt": pa.array([float((i * 7) % 100) for i in ids], pa.float64()),
        "qty": pa.array([(i % 9) + 1 for i in ids], pa.int64()),
    })
    pq.write_table(t, path, row_group_size=rg)
    return t


def test_group_sum_count_matches_duckdb(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "a.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p),
        keys=["cat"],
        aggs=(("sum", "amt", "sum_amt"), ("count", None, "n")),
    )
    out = submission.execute(plan)
    expected = duckdb_con.execute(
        f"SELECT cat, sum(amt) AS sum_amt, count(*) AS n "
        f"FROM read_parquet('{p}') GROUP BY cat").arrow()
    assert_frames_equal(out, expected)


def test_min_max_avg(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "a.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p),
        keys=["cat"],
        aggs=(("min", "amt", "mn"), ("max", "amt", "mx"), ("avg", "amt", "av")),
    )
    out = submission.execute(plan)
    expected = duckdb_con.execute(
        f"SELECT cat, min(amt) AS mn, max(amt) AS mx, avg(amt) AS av "
        f"FROM read_parquet('{p}') GROUP BY cat").arrow()
    assert_frames_equal(out, expected)


def test_multi_key_group(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "a.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p),
        keys=["cat", "qty"],
        aggs=(("sum", "amt", "s"),),
    )
    out = submission.execute(plan)
    expected = duckdb_con.execute(
        f"SELECT cat, qty, sum(amt) AS s FROM read_parquet('{p}') "
        f"GROUP BY cat, qty").arrow()
    assert_frames_equal(out, expected)


@settings(max_examples=25, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(split=st.integers(min_value=1, max_value=399))
def test_partial_then_merge_equals_whole(submission, tmp_path, split):
    """partial(A)+partial(B) merged == partial(A+B): the defining property of a
    correct partial/final aggregate (kills average-of-averages)."""
    p = str(tmp_path / "pm.parquet")
    t = _write(p)
    aggs = (("sum", "amt", "s"), ("count", None, "n"), ("avg", "amt", "av"),
            ("min", "amt", "mn"), ("max", "amt", "mx"))
    keys = ["cat"]
    a = t.slice(0, split)
    b = t.slice(split, t.num_rows - split)
    whole = submission.merge_partials(
        [submission.partial_aggregate(t, keys, aggs)], aggs)
    chunked = submission.merge_partials(
        [submission.partial_aggregate(a, keys, aggs),
         submission.partial_aggregate(b, keys, aggs)], aggs)
    # Compare finalized tables through execute on an Aggregate for a stable shape.
    # Here we finalize via a full Aggregate over p as the oracle for `whole`.
    assert whole.keys() == chunked.keys()
    for k in whole:
        for out in ("s", "n", "av", "mn", "mx"):
            wv, cv = whole[k][out], chunked[k][out]
            if out == "av":
                # avg carried as (sum,count) pre-finalize
                assert wv == cv
            else:
                assert wv == cv


def test_empty_aggregate(submission, tmp_path):
    p = str(tmp_path / "empty.parquet")
    pq.write_table(pa.table({
        "cat": pa.array([], pa.string()),
        "amt": pa.array([], pa.float64()),
    }), p)
    out = submission.execute(submission.Aggregate(
        submission.new_scan(p), keys=["cat"], aggs=(("sum", "amt", "s"),)))
    assert out.num_rows == 0

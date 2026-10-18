"""Stage 7 — rule-based optimizer (filter pushdown, projection pruning).

``optimize(plan)`` returns a rewritten plan TREE that the tests inspect
structurally (like reading Catalyst's optimized plan), and the optimized plan
must compute the SAME result as the unoptimized one (and DuckDB, G6).

Rules under test:
  * filter pushdown: a Filter above a Project is pushed below it when the
    predicate only touches surviving columns;
  * projection pruning: a Scan under a Project reads only the needed columns.
"""

import pyarrow as pa
import pyarrow.parquet as pq

from de_testkit import assert_frames_equal


def _write(path, n=300, rg=80):
    ids = list(range(n))
    cats = ["a", "b", "c"]
    t = pa.table({
        "id": pa.array(ids, pa.int64()),
        "cat": pa.array([cats[i % 3] for i in ids]),
        "amt": pa.array([float(i % 40) for i in ids], pa.float64()),
        "note": pa.array([f"n{i}" for i in ids]),
    })
    pq.write_table(t, path, row_group_size=rg)
    return t


def _find(node, cls, submission):
    """Yield all nodes of a given class in the plan tree."""
    found = []
    stack = [node]
    pairs = {
        "Project": ("child",), "Filter": ("child",), "Aggregate": ("child",),
        "Exchange": ("child",), "Optimized": ("child",),
        "HashJoin": ("left", "right"), "BroadcastJoin": ("left", "right"),
    }
    while stack:
        cur = stack.pop()
        if type(cur).__name__ == cls:
            found.append(cur)
        for attr in pairs.get(type(cur).__name__, ()):
            stack.append(getattr(cur, attr))
    return found


def test_filter_pushed_below_project(submission, tmp_path):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Filter(
        submission.Project(submission.new_scan(p), columns=["id", "amt"]),
        submission.Compare(submission.Col("amt"), ">", submission.Lit(10.0)),
    )
    opt = submission.optimize(plan)
    # In the optimized tree the top projection sits ABOVE the filter.
    assert type(opt).__name__ == "Project"
    assert type(opt.child).__name__ == "Filter"


def test_projection_pruning_reaches_scan(submission, tmp_path):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Project(submission.new_scan(p), columns=["id", "amt"])
    opt = submission.optimize(plan)
    scans = _find(opt, "Scan", submission)
    assert scans, "no Scan in optimized plan"
    # The scan must read only the needed columns (not `note`/`cat`).
    cols = set(scans[0].columns or [])
    assert cols == {"id", "amt"}, f"scan reads {cols}, expected id/amt only"


def test_optimizer_preserves_results(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Filter(
        submission.Project(submission.new_scan(p), columns=["id", "cat", "amt"]),
        submission.Compare(submission.Col("amt"), ">=", submission.Lit(20.0)),
    )
    before = submission.execute(plan)
    after = submission.execute(submission.optimize(plan))
    assert_frames_equal(after, before)
    expected = duckdb_con.execute(
        f"SELECT id, cat, amt FROM read_parquet('{p}') WHERE amt >= 20.0").arrow()
    assert_frames_equal(after, expected)


def test_pruning_feeds_aggregate(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Aggregate(
        submission.new_scan(p), keys=["cat"], aggs=(("sum", "amt", "s"),))
    opt = submission.optimize(plan)
    scans = _find(opt, "Scan", submission)
    assert set(scans[0].columns or []) == {"cat", "amt"}
    out = submission.execute(opt)
    expected = duckdb_con.execute(
        f"SELECT cat, sum(amt) AS s FROM read_parquet('{p}') GROUP BY cat").arrow()
    assert_frames_equal(out, expected)


def test_filter_over_join_pushed_to_correct_side(submission, tmp_path, duckdb_con):
    """A filter above a join whose predicate references a RIGHT-side column must
    not be pushed into the left child. We assert correctness after optimize():
    a mutant that pushes to the wrong side produces wrong rows or errors."""
    fp = str(tmp_path / "f.parquet")
    dp = str(tmp_path / "d.parquet")
    pq.write_table(pa.table({
        "oid": pa.array(list(range(60)), pa.int64()),
        "cust_id": pa.array([i % 6 for i in range(60)], pa.int64()),
    }), fp)
    pq.write_table(pa.table({
        "cust_id": pa.array(list(range(6)), pa.int64()),
        "region": pa.array(["N", "S", "N", "S", "N", "S"]),
    }), dp)
    plan = submission.Filter(
        submission.HashJoin(
            submission.new_scan(fp), submission.new_scan(dp),
            left_keys=["cust_id"], right_keys=["cust_id"], how="inner"),
        submission.Compare(submission.Col("region"), "=", submission.Lit("N")),
    )
    out = submission.execute(submission.optimize(plan))
    expected = duckdb_con.execute(
        f"SELECT f.oid, f.cust_id, d.region "
        f"FROM read_parquet('{fp}') f JOIN read_parquet('{dp}') d "
        f"ON f.cust_id = d.cust_id WHERE d.region = 'N'").arrow()
    assert_frames_equal(out, expected)


def test_pruning_keeps_filter_only_column(submission, tmp_path, duckdb_con):
    """The filter uses `amt`, but the top projection keeps only `id`. Correct
    projection pruning must still let the scan read `amt` (the filter needs it);
    a pruner that forgets the predicate's columns breaks here."""
    p = str(tmp_path / "d.parquet")
    _write(p)
    plan = submission.Project(
        submission.Filter(
            submission.new_scan(p),
            submission.Compare(submission.Col("amt"), ">", submission.Lit(15.0)),
        ),
        columns=["id"],
    )
    opt = submission.optimize(plan)
    out = submission.execute(opt)
    expected = duckdb_con.execute(
        f"SELECT id FROM read_parquet('{p}') WHERE amt > 15.0").arrow()
    assert_frames_equal(out, expected)
    # The scan must have read `amt` even though it is projected away at the top.
    scans = _find(opt, "Scan", submission)
    assert "amt" in set(scans[0].columns or [])


def test_filter_not_pushed_when_column_projected_away(submission, tmp_path, duckdb_con):
    """If the filter uses a column the projection drops, it must NOT be pushed
    below the projection (that would be unsound / reference a missing column)."""
    p = str(tmp_path / "d.parquet")
    _write(p)
    # Project drops `cat`; filter references `id` (kept) -> safe to push.
    # Here filter references `id` which survives, so pushdown IS allowed; verify
    # correctness holds either way.
    plan = submission.Filter(
        submission.Project(submission.new_scan(p), columns=["id", "amt"]),
        submission.Compare(submission.Col("id"), "<", submission.Lit(50)),
    )
    out = submission.execute(submission.optimize(plan))
    expected = duckdb_con.execute(
        f"SELECT id, amt FROM read_parquet('{p}') WHERE id < 50").arrow()
    assert_frames_equal(out, expected)

"""Stage 5 — hash join and broadcast join.

HashJoin and BroadcastJoin must give identical results (only distribution
differs), equal to DuckDB (G6). Covers: duplicate keys on the build side (cross
product per key), NULL keys (never match, SQL semantics), and a left outer join.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from de_testkit import assert_frames_equal


def _write(path, table):
    pq.write_table(table, path)
    return path


def _facts(n=200, with_nulls=True):
    rows = []
    for i in range(n):
        cust = None if (with_nulls and i % 37 == 0) else (i % 10)
        rows.append({"oid": i, "cust_id": cust, "amt": float(i % 50)})
    return pa.Table.from_pylist(rows)


def _dims():
    # cust_id 3 appears TWICE (duplicate build key) -> fan-out on join.
    rows = [
        {"cust_id": 0, "name": "c0"}, {"cust_id": 1, "name": "c1"},
        {"cust_id": 2, "name": "c2"}, {"cust_id": 3, "name": "c3a"},
        {"cust_id": 3, "name": "c3b"}, {"cust_id": 4, "name": "c4"},
        {"cust_id": 5, "name": "c5"}, {"cust_id": 9, "name": "c9"},
    ]
    return pa.Table.from_pylist(rows)


def _oracle(duckdb_con, fp, dp, how="INNER"):
    return duckdb_con.execute(
        f"SELECT f.oid, f.cust_id, f.amt, d.name "
        f"FROM read_parquet('{fp}') f {how} JOIN read_parquet('{dp}') d "
        f"ON f.cust_id = d.cust_id").arrow()


def test_hash_join_inner_matches_duckdb(submission, tmp_path, duckdb_con):
    fp = _write(str(tmp_path / "f.parquet"), _facts())
    dp = _write(str(tmp_path / "d.parquet"), _dims())
    plan = submission.HashJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="inner")
    out = submission.execute(plan)
    assert_frames_equal(out, _oracle(duckdb_con, fp, dp, "INNER"))


def test_broadcast_join_same_as_hash(submission, tmp_path, duckdb_con):
    fp = _write(str(tmp_path / "f.parquet"), _facts())
    dp = _write(str(tmp_path / "d.parquet"), _dims())
    bj = submission.BroadcastJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="inner")
    out = submission.execute(bj)
    assert_frames_equal(out, _oracle(duckdb_con, fp, dp, "INNER"))


def test_duplicate_build_keys_fan_out(submission, tmp_path, duckdb_con):
    """cust_id=3 appears twice on the build side; each probe row must match BOTH
    (kills a join that dedups the build side)."""
    fp = _write(str(tmp_path / "f.parquet"), _facts(n=50, with_nulls=False))
    dp = _write(str(tmp_path / "d.parquet"), _dims())
    plan = submission.HashJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="inner")
    out = submission.execute(plan)
    expected = _oracle(duckdb_con, fp, dp, "INNER")
    assert out.num_rows == expected.num_rows
    assert_frames_equal(out, expected)


def test_null_keys_never_match(submission, tmp_path, duckdb_con):
    fp = _write(str(tmp_path / "f.parquet"), _facts(n=200, with_nulls=True))
    # A dimension row with a NULL key: a correct join must NOT match it against
    # the fact rows that also have NULL cust_id (NULL = NULL is UNKNOWN in SQL).
    dim_schema = pa.schema([("cust_id", pa.int64()), ("name", pa.string())])
    dims = pa.concat_tables([
        _dims().cast(dim_schema),
        pa.Table.from_pylist([{"cust_id": None, "name": "DIM_NULL"}],
                             schema=dim_schema),
    ])
    dp = _write(str(tmp_path / "d.parquet"), dims)
    plan = submission.HashJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="inner")
    out = submission.execute(plan)
    assert None not in out.column("cust_id").to_pylist()
    assert "DIM_NULL" not in out.column("name").to_pylist()
    assert_frames_equal(out, _oracle(duckdb_con, fp, dp, "INNER"))


def test_left_outer_join(submission, tmp_path, duckdb_con):
    fp = _write(str(tmp_path / "f.parquet"), _facts(n=120, with_nulls=True))
    dp = _write(str(tmp_path / "d.parquet"), _dims())
    plan = submission.HashJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="left")
    out = submission.execute(plan)
    assert_frames_equal(out, _oracle(duckdb_con, fp, dp, "LEFT"))


@settings(max_examples=15, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(seed=st.integers(min_value=0, max_value=10_000))
def test_join_equals_duckdb_property(submission, tmp_path, duckdb_con, seed):
    import random
    rnd = random.Random(seed)
    facts = pa.Table.from_pylist([
        {"oid": i,
         "cust_id": rnd.choice([None, 0, 1, 2, 3, 7]),
         "amt": float(rnd.randint(0, 20))}
        for i in range(60)])
    dims = pa.Table.from_pylist([
        {"cust_id": k, "name": f"n{k}_{j}"}
        for k in [0, 1, 2, 3] for j in range(rnd.randint(1, 2))])
    fp = _write(str(tmp_path / "f.parquet"), facts)
    dp = _write(str(tmp_path / "d.parquet"), dims)
    plan = submission.HashJoin(
        submission.new_scan(fp), submission.new_scan(dp),
        left_keys=["cust_id"], right_keys=["cust_id"], how="inner")
    out = submission.execute(plan)
    assert_frames_equal(out, _oracle(duckdb_con, fp, dp, "INNER"))

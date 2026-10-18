"""Stage 1 — scan Parquet in row-group batches.

Black-box against the public API: build a Scan plan node and run it through
execute()/explain(). The oracle is DuckDB over the same Parquet file (G6).
"""

import pyarrow as pa
import pyarrow.parquet as pq

from de_testkit import assert_frames_equal


def _write_parquet(path, n=300, row_group_size=100):
    ids = list(range(n))
    cat = ["a", "b", "c"]
    t = pa.table({
        "id": pa.array(ids, pa.int64()),
        "cat": pa.array([cat[i % 3] for i in ids]),
        "amt": pa.array([float(i) for i in ids], pa.float64()),
    })
    pq.write_table(t, path, row_group_size=row_group_size)
    return t


def test_scan_matches_duckdb_oracle(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "data.parquet")
    _write_parquet(p)
    plan = submission.new_scan(p)
    out = submission.execute(plan)
    assert isinstance(out, pa.Table)
    expected = duckdb_con.execute(f"SELECT * FROM read_parquet('{p}')").arrow()
    assert_frames_equal(out, expected)


def test_scan_reads_multiple_row_groups(submission, tmp_path):
    p = str(tmp_path / "data.parquet")
    _write_parquet(p, n=300, row_group_size=100)
    assert pq.ParquetFile(p).num_row_groups == 3
    plan = submission.new_scan(p)
    out = submission.execute(plan)
    assert out.num_rows == 300  # all row groups read, not just the first


def test_scan_column_projection(submission, tmp_path, duckdb_con):
    p = str(tmp_path / "data.parquet")
    _write_parquet(p)
    plan = submission.new_scan(p, columns=["id", "amt"])
    out = submission.execute(plan)
    assert set(out.column_names) == {"id", "amt"}
    expected = duckdb_con.execute(
        f"SELECT id, amt FROM read_parquet('{p}')").arrow()
    assert_frames_equal(out, expected)


def test_explain_mentions_scan(submission, tmp_path):
    p = str(tmp_path / "data.parquet")
    _write_parquet(p)
    plan = submission.new_scan(p)
    text = submission.explain(plan)
    assert isinstance(text, str)
    assert "Scan" in text


def test_scan_empty_file(submission, tmp_path):
    p = str(tmp_path / "empty.parquet")
    pq.write_table(pa.table({"id": pa.array([], pa.int64())}), p)
    out = submission.execute(submission.new_scan(p))
    assert out.num_rows == 0

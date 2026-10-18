"""Tests for SPK-07 / 01-target-file-count.

File count: exactly num_files part-*.parquet files are written (tested at 2 and 4).
Data (G6): reading the output back equals the input rows (DuckDB oracle on the
source parquet).
"""

import glob
import os

from de_testkit import assert_frames_equal


def _part_files(out_dir):
    return [
        f for f in glob.glob(os.path.join(out_dir, "*.parquet"))
        if os.path.basename(f).startswith("part-")
    ]


def _orders(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["orders"]))


def _oracle(con, path):
    return con.execute(f"SELECT * FROM read_parquet('{path}')").arrow()


def test_exact_file_count_2(submission, spark, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_balanced(df, str(tmp_path / "o2"), 2)
    assert len(_part_files(out)) == 2, _part_files(out)


def test_exact_file_count_4(submission, spark, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_balanced(df, str(tmp_path / "o4"), 4)
    assert len(_part_files(out)) == 4, _part_files(out)


def test_data_roundtrip(submission, spark, duckdb_con, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_balanced(df, str(tmp_path / "orx"), 3)
    written = spark.read.parquet(out)
    assert_frames_equal(written, _oracle(duckdb_con, commerce_data["parquet"]["orders"]))


def test_returns_path(submission, spark, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_balanced(df, str(tmp_path / "op"), 2)
    assert isinstance(out, str) and os.path.isdir(out)

"""Tests for SPK-07 / 03-partition-by-layout.

Layout: exactly one company_id=<value>/ directory per distinct company (and no
more — a high-cardinality partition column would create far more).
Data (G6): the full read-back equals the input; a per-company filter equals that
company's oracle rows (partition pruning).
"""

import glob
import os

from de_testkit import assert_frames_equal


def _orders(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["orders"]))


def _part_dirs(out):
    return sorted(
        d for d in glob.glob(os.path.join(out, "company_id=*")) if os.path.isdir(d)
    )


def test_partition_dir_layout(submission, spark, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_partitioned(df, str(tmp_path / "p"))
    dirs = _part_dirs(out)
    n_companies = df.select("company_id").distinct().count()
    assert len(dirs) == n_companies, (
        f"Expected {n_companies} company_id=.../ dirs, found {len(dirs)}: {dirs}"
    )


def test_full_roundtrip(submission, spark, duckdb_con, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_partitioned(df, str(tmp_path / "p2"))
    back = spark.read.parquet(out)
    oracle = duckdb_con.execute(
        f"SELECT * FROM read_parquet('{commerce_data['parquet']['orders']}')"
    ).arrow()
    assert_frames_equal(back, oracle)


def test_partition_pruning(submission, spark, duckdb_con, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_partitioned(df, str(tmp_path / "p3"))
    back = spark.read.parquet(out).filter("company_id = 'C001'")
    oracle = duckdb_con.execute(
        f"""SELECT * FROM read_parquet('{commerce_data['parquet']['orders']}')
            WHERE company_id = 'C001'"""
    ).arrow()
    assert_frames_equal(back, oracle)


def test_returns_path(submission, spark, tmp_path, commerce_data):
    df = _orders(spark, commerce_data)
    out = submission.write_partitioned(df, str(tmp_path / "p4"))
    assert isinstance(out, str) and os.path.isdir(out)

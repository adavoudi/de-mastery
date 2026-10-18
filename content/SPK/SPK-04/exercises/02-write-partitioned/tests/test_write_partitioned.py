"""Tests for SPK-04 / 02-write-partitioned.

Oracle for correctness: the read-back must equal the input DataFrame (compared via
DuckDB-independent set equality through assert_frames_equal). Partitioning and save-mode
behavior are checked structurally and by re-running the writer.
"""

from pathlib import Path

from de_testkit import assert_frames_equal


def _orders(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["orders"]))


def test_roundtrip_equals_input(submission, spark, commerce_data, tmp_path):
    df = _orders(spark, commerce_data)
    out = str(tmp_path / "out1")
    submission.write_partitioned(df, out)
    back = spark.read.parquet(out)
    # same columns and same rows (order ignored); partition column is restored on read
    assert_frames_equal(back, df)


def test_partition_directories_exist(submission, spark, commerce_data, tmp_path):
    df = _orders(spark, commerce_data)
    out = tmp_path / "out2"
    submission.write_partitioned(df, str(out))
    dirs = sorted(p.name for p in out.iterdir() if p.is_dir())
    assert dirs == ["company_id=C001", "company_id=C002", "company_id=C003"], dirs


def test_rerun_is_idempotent(submission, spark, commerce_data, tmp_path):
    # overwrite => running twice leaves exactly one copy. append would double it.
    df = _orders(spark, commerce_data)
    out = str(tmp_path / "out3")
    submission.write_partitioned(df, out)
    first = spark.read.parquet(out).count()
    submission.write_partitioned(df, out)
    second = spark.read.parquet(out).count()
    assert first == df.count(), f"first write wrote {first}, expected {df.count()}"
    assert second == df.count(), f"re-run doubled/changed the data: {second}"

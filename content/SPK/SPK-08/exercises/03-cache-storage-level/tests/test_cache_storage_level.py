"""Tests for SPK-08 / 03-cache-storage-level.

Storage level: df.storageLevel equals the requested StorageLevel (API check, not
a wall-clock measurement). Data: DuckDB sum oracle. Reuse: two actions agree.
"""

import pytest
from pyspark import StorageLevel

from de_testkit import assert_frames_equal

LEVELS = ["MEMORY_AND_DISK", "DISK_ONLY"]


def _items(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["order_items"]))


def _oracle(con, path):
    return con.execute(
        f"SELECT company_id, sum(line_amount) AS total FROM read_parquet('{path}') GROUP BY company_id"
    ).arrow()


@pytest.mark.parametrize("level", LEVELS)
def test_storage_level_matches(submission, spark, commerce_data, level):
    out = submission.persist_totals(spark, _items(spark, commerce_data), level)
    try:
        assert out.storageLevel == getattr(StorageLevel, level), (
            f"storageLevel {out.storageLevel} != requested {level}"
        )
    finally:
        out.unpersist()


@pytest.mark.parametrize("level", LEVELS)
def test_matches_oracle(submission, spark, duckdb_con, commerce_data, level):
    out = submission.persist_totals(spark, _items(spark, commerce_data), level)
    try:
        assert_frames_equal(out, _oracle(duckdb_con, commerce_data["parquet"]["order_items"]),
                            rel_tol=1e-6)
    finally:
        out.unpersist()


def test_reuse_is_consistent(submission, spark, commerce_data):
    out = submission.persist_totals(spark, _items(spark, commerce_data), "MEMORY_AND_DISK")
    try:
        a = out.count()
        b = out.count()
        assert a == b and a > 0
    finally:
        out.unpersist()

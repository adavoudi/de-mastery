"""de_testkit helpers: frame equality, plan assertions, java resolution."""

import pyarrow as pa
import pytest

from de_testkit.asserts import (
    assert_frames_equal,
    assert_plan_contains,
    count_exchanges,
)


def test_frames_equal_arrow_order_insensitive():
    a = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    b = pa.table({"y": ["c", "b", "a"], "x": [3, 2, 1]})
    assert_frames_equal(a, b)  # columns + rows reordered, still equal


def test_frames_equal_float_tolerance():
    a = pa.table({"v": [1.0000000001]})
    b = pa.table({"v": [1.0]})
    assert_frames_equal(a, b, rel_tol=1e-6)


def test_frames_equal_detects_mismatch():
    a = pa.table({"x": [1, 2]})
    b = pa.table({"x": [1, 3]})
    with pytest.raises(AssertionError):
        assert_frames_equal(a, b)


def test_frames_equal_list_of_dicts():
    a = [{"id": 1, "n": "x"}, {"id": 2, "n": "y"}]
    b = [{"n": "y", "id": 2}, {"n": "x", "id": 1}]
    assert_frames_equal(a, b)


def test_frames_equal_column_mismatch_message():
    a = pa.table({"x": [1]})
    b = pa.table({"z": [1]})
    with pytest.raises(AssertionError) as ei:
        assert_frames_equal(a, b)
    assert "Column mismatch" in str(ei.value)


# --- Spark plan assertion (skips cleanly if Java/Spark unusable) ---
def test_spark_plan_contains_broadcast(spark):
    from pyspark.sql.functions import broadcast

    big = spark.range(0, 1000).toDF("id")
    small = spark.range(0, 5).toDF("id")
    joined = big.join(broadcast(small), "id")
    assert_plan_contains(joined, "BroadcastHashJoin")


def test_count_exchanges_on_groupby(spark):
    df = spark.range(0, 100).toDF("id")
    agg = df.groupBy((df.id % 10).alias("k")).count()
    # A shuffle (Exchange) is expected for the hash aggregation.
    assert count_exchanges(agg) >= 1


def test_assert_frames_equal_spark_vs_duckdb(spark, duckdb_con):
    sdf = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    duckdb_con.execute("create table t(id int, name varchar)")
    duckdb_con.executemany("insert into t values (?,?)", [(2, "b"), (1, "a")])
    expected = duckdb_con.execute("select * from t").arrow()
    assert_frames_equal(sdf, expected)

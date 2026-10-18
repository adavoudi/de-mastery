"""Tests for SPK-07 / 02-coalesce-no-shuffle.

The base is pre-shuffled to 8 partitions (so there IS something to coalesce). We
record the base plan's Exchange count; a correct (coalesce) reduction adds none,
while repartition would add one. Partition count must land on num_files, and the
data must equal the USD oracle.
"""

import pytest
from pyspark.sql import functions as F

from de_testkit import assert_frames_equal, count_exchanges, plan_of

NUM_FILES = 2


@pytest.fixture
def base(spark, commerce_data):
    # Shuffle to many partitions so coalesce has partitions to merge.
    return spark.read.parquet(str(commerce_data["parquet"]["orders"])).repartition(8)


def _oracle(con, path):
    return con.execute(
        f"SELECT * FROM read_parquet('{path}') WHERE currency = 'USD'"
    ).arrow()


def test_matches_oracle(submission, base, duckdb_con, commerce_data):
    out = submission.reduce_usd(base, NUM_FILES)
    assert_frames_equal(out, _oracle(duckdb_con, commerce_data["parquet"]["orders"]))


def test_partition_count(submission, base):
    out = submission.reduce_usd(base, NUM_FILES)
    assert out.rdd.getNumPartitions() == NUM_FILES


def test_no_added_exchange(submission, base):
    base_exch = count_exchanges(base, "formatted")
    out = submission.reduce_usd(base, NUM_FILES)
    plan = plan_of(out, "formatted")
    out_exch = plan.count("Exchange")
    # coalesce is narrow: it adds no Exchange AND appears as a `Coalesce` operator.
    # repartition would instead introduce a RoundRobinPartitioning Exchange and no
    # Coalesce node (adjacent repartitions collapse, so counting alone is not enough).
    assert out_exch <= base_exch, (
        f"The reduction added an Exchange (shuffle). Plan:\n{plan}"
    )
    assert "Coalesce" in plan, (
        "Expected a narrow `Coalesce` operator in the plan. A `repartition` would "
        f"shuffle (RoundRobinPartitioning) instead.\n--- plan ---\n{plan}"
    )

"""Tests for SPK-08 / 02-aqe-coalesce.

We reset AQE OFF before each call so the learner's function must enable it. After
an action, the FINAL plan must show AdaptiveSparkPlan(isFinalPlan=true) and a
coalesced AQEShuffleRead. Data must match the DuckDB sum oracle.

The shared session starts with AQE off; these runtime SQL confs are per-session
and are reset here, so this suite does not affect other exercises' subprocesses.
"""

import pytest

from de_testkit import assert_frames_equal, plan_of


@pytest.fixture(autouse=True)
def _aqe_off_before(spark):
    # Baseline: AQE off, small shuffle partitions (like the default fixture state).
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    yield
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")


def _items(spark, commerce_data):
    return spark.read.parquet(str(commerce_data["parquet"]["order_items"]))


def _oracle(con, path):
    return con.execute(
        f"SELECT company_id, sum(line_amount) AS total FROM read_parquet('{path}') GROUP BY company_id"
    ).arrow()


def test_matches_oracle(submission, spark, duckdb_con, commerce_data):
    out = submission.coalesced_aggregate(spark, _items(spark, commerce_data))
    assert_frames_equal(out, _oracle(duckdb_con, commerce_data["parquet"]["order_items"]),
                        rel_tol=1e-6)


def test_aqe_in_final_plan(submission, spark, commerce_data):
    out = submission.coalesced_aggregate(spark, _items(spark, commerce_data))
    out.collect()  # run it so AQE finalizes the plan
    plan = plan_of(out, "formatted")
    assert "AdaptiveSparkPlan" in plan, f"AQE not enabled; plan:\n{plan}"
    assert "isFinalPlan=true" in plan, f"Final AQE plan not reached; plan:\n{plan}"


def test_coalesced_shuffle_read(submission, spark, commerce_data):
    out = submission.coalesced_aggregate(spark, _items(spark, commerce_data))
    n = out.rdd.getNumPartitions()
    plan = plan_of(out, "formatted")
    # With coalescing on, the ~64 small post-shuffle partitions collapse to very
    # few (1 at this scale). Without it, the result keeps the configured count.
    assert n <= 8, (
        f"Expected AQE to coalesce the shuffle partitions to a small number, "
        f"got {n}. Enable coalescePartitions and raise shuffle.partitions.\n{plan}"
    )
    assert "coalesced" in plan.lower(), (
        f"No coalesced AQEShuffleRead in the final plan.\n{plan}"
    )

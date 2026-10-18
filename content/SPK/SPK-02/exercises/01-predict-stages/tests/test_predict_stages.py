"""Tests for SPK-02 / 01-predict-stages.

The expected stage count is NOT hand-written: for each query we build the DataFrame,
read its executed physical plan, count the Exchange (shuffle) nodes, and compute
stages = exchanges + 1. The learner's prediction must match that truth.

Broadcast joins are disabled so joins deterministically shuffle both sides.
"""

import pytest

QUERY_IDS = [
    "q1_filter_select",
    "q2_group_count",
    "q3_group_then_sort",
    "q4_distinct",
    "q5_join",
    "q6_join_then_group",
]


def _exchanges(df) -> int:
    # Count Exchange nodes in the actual executed physical plan (AQE is off in the
    # testkit session, so this is stable).
    plan = df._jdf.queryExecution().executedPlan().toString()
    return plan.count("Exchange")


@pytest.fixture
def queries(spark, commerce_data):
    orders = spark.read.parquet(str(commerce_data["parquet"]["orders"]))
    items = spark.read.parquet(str(commerce_data["parquet"]["order_items"]))
    return {
        "q1_filter_select": orders.filter("currency = 'USD'").select("order_id", "currency"),
        "q2_group_count": orders.groupBy("currency").count(),
        "q3_group_then_sort": orders.groupBy("currency").count().orderBy("currency"),
        "q4_distinct": orders.select("currency").distinct(),
        "q5_join": orders.join(items, "order_id"),
        "q6_join_then_group": orders.join(items, "order_id").groupBy(items["company_id"]).count(),
    }


@pytest.fixture(autouse=True)
def _no_broadcast(spark):
    prev = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    yield
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", prev)


def test_predictions_have_all_ids(submission):
    pred = submission.predicted_stages()
    assert set(pred.keys()) == set(QUERY_IDS), f"keys were {sorted(pred.keys())}"
    assert all(isinstance(v, int) and v >= 1 for v in pred.values())


def test_each_prediction_matches_actual_plan(submission, queries):
    pred = submission.predicted_stages()
    for qid, df in queries.items():
        truth = _exchanges(df) + 1   # stages = shuffles + 1 (linear plan)
        assert pred[qid] == truth, (
            f"{qid}: predicted {pred[qid]} stages but the plan has "
            f"{truth} (exchanges={truth - 1})")

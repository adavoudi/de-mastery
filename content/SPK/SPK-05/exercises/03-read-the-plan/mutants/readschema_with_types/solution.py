"""Mutant: fails to strip the `:type` suffix from ReadSchema fields, so
pruned_columns contains 'order_id:bigint' instead of 'order_id'."""

import re


def _plan(df):
    return df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode
        .fromString("formatted")
    )


def plan_facts(df):
    plan = _plan(df)
    pushed = any(
        b.strip() for b in re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    )
    pruned = set()
    for block in re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan):
        pruned |= {f.strip() for f in block.split(",") if f.strip()}  # keeps :type
    return {
        "pushed_down": pushed,
        "pruned_columns": pruned,
        "num_shuffles": plan.count("Exchange"),
    }

"""Mutant: assumes pushdown always happens. Fails the case where a UDF filter
blocks pushdown (PushedFilters empty)."""

import re


def _plan(df):
    return df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode
        .fromString("formatted")
    )


def plan_facts(df):
    plan = _plan(df)
    pruned = set()
    for block in re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan):
        pruned |= {f.strip().split(":")[0] for f in block.split(",") if f.strip()}
    return {
        "pushed_down": True,  # wrong: ignores the actual PushedFilters
        "pruned_columns": pruned,
        "num_shuffles": plan.count("Exchange"),
    }

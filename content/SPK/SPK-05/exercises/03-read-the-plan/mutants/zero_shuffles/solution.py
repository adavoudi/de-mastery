"""Mutant: never counts shuffles (returns 0). Fails the groupBy/join cases that
have one or more Exchange nodes."""

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
        pruned |= {f.strip().split(":")[0] for f in block.split(",") if f.strip()}
    return {
        "pushed_down": pushed,
        "pruned_columns": pruned,
        "num_shuffles": 0,  # wrong: ignores Exchange nodes
    }

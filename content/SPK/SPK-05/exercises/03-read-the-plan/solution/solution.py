"""SPK-05 / 03-read-the-plan — reference solution.

Parse the formatted physical plan string for the three facts.
"""

import re


def _plan(df) -> str:
    return df._jdf.queryExecution().explainString(
        df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode
        .fromString("formatted")
    )


def plan_facts(df):
    plan = _plan(df)

    pushed_lists = re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    pushed_down = any(block.strip() for block in pushed_lists)

    pruned = set()
    for block in re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan):
        for field in block.split(","):
            field = field.strip()
            if field:
                pruned.add(field.split(":")[0])

    num_shuffles = plan.count("Exchange")

    return {
        "pushed_down": pushed_down,
        "pruned_columns": pruned,
        "num_shuffles": num_shuffles,
    }

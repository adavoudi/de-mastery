"""SPK-05 / 03 — alt: use the de_testkit plan_of / count_exchanges helpers."""

import re

from de_testkit import count_exchanges, plan_of


def plan_facts(df):
    plan = plan_of(df, "formatted")
    pushed = any(
        b.strip() for b in re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    )
    pruned = set()
    for block in re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan):
        pruned |= {f.strip().split(":")[0] for f in block.split(",") if f.strip()}
    return {
        "pushed_down": pushed,
        "pruned_columns": pruned,
        "num_shuffles": count_exchanges(df, "formatted"),
    }

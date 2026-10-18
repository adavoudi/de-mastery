"""SPK-05 / 03 — alt: capture df.explain('formatted') stdout and parse that."""

import contextlib
import io
import re


def _plan(df) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain("formatted")
    return buf.getvalue()


def plan_facts(df):
    plan = _plan(df)
    pushed = any(
        b.strip() for b in re.findall(r"PushedFilters:\s*\[(.*?)\]", plan)
    )
    cols = set()
    for block in re.findall(r"ReadSchema:\s*struct<([^>]*)>", plan):
        for f in block.split(","):
            f = f.strip()
            if f:
                cols.add(f.split(":")[0])
    return {
        "pushed_down": pushed,
        "pruned_columns": cols,
        "num_shuffles": plan.count("Exchange"),
    }

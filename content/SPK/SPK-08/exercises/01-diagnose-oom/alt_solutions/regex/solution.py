"""SPK-08 / 01 — alt: regex-driven classification."""

import re


def diagnose(log):
    t = log.lower()
    if re.search(r"spilling (in-memory|sort)", t):
        return {"location": "executor", "cause": "spill",
                "fix": "more_partitions_or_memory"}
    if re.search(r"(exceeding memory limits|oomkilled|physical memory used)", t):
        return {"location": "executor", "cause": "overhead",
                "fix": "raise_overhead"}
    if re.search(r"outofmemoryerror", t) and re.search(r"(collect|topandas|\.take\()", t):
        return {"location": "driver", "cause": "collect_to_driver",
                "fix": "avoid_collect"}
    if re.search(r"outofmemoryerror", t):
        return {"location": "executor", "cause": "skew",
                "fix": "salt_or_aqe_skew"}
    raise ValueError(log)

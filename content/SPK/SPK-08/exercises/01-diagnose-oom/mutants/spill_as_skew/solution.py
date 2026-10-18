"""Mutant: treats a (non-fatal) spill as a skew OOM — wrong cause and fix for the
spill case (it is execution-memory pressure, not skew)."""


def diagnose(log):
    t = log.lower()
    if "exceeding memory limits" in t or "oomkilled" in t or "physical memory used" in t:
        return {"location": "executor", "cause": "overhead", "fix": "raise_overhead"}
    if "outofmemoryerror" in t and ("collect" in t or "topandas" in t or ".take(" in t):
        return {"location": "driver", "cause": "collect_to_driver", "fix": "avoid_collect"}
    # BUG: spill falls through to the generic executor-OOM/skew branch.
    return {"location": "executor", "cause": "skew", "fix": "salt_or_aqe_skew"}

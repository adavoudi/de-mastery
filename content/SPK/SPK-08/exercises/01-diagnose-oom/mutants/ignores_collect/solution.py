"""Mutant: ignores the collect/toPandas signal, so a driver-collect OOM is
misclassified as an executor skew OOM (wrong location, cause and fix)."""


def diagnose(log):
    t = log.lower()
    if "spilling in-memory" in t or "spilling sort" in t:
        return {"location": "executor", "cause": "spill", "fix": "more_partitions_or_memory"}
    if "exceeding memory limits" in t or "oomkilled" in t or "physical memory used" in t:
        return {"location": "executor", "cause": "overhead", "fix": "raise_overhead"}
    # BUG: never checks for collect/toPandas; all remaining OOMs => skew.
    return {"location": "executor", "cause": "skew", "fix": "salt_or_aqe_skew"}

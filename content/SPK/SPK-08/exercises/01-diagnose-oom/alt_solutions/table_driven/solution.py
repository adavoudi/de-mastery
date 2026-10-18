"""SPK-08 / 01 — alt: table-driven. Ordered rules of (predicate -> diagnosis)."""

_DIAG = {
    "spill": {"location": "executor", "cause": "spill",
              "fix": "more_partitions_or_memory"},
    "overhead": {"location": "executor", "cause": "overhead",
                 "fix": "raise_overhead"},
    "driver": {"location": "driver", "cause": "collect_to_driver",
               "fix": "avoid_collect"},
    "skew": {"location": "executor", "cause": "skew",
             "fix": "salt_or_aqe_skew"},
}


def diagnose(log):
    t = log.lower()
    has_oom = "outofmemoryerror" in t
    rules = [
        ("spill", "spilling in-memory" in t or "spilling sort" in t),
        ("overhead", "exceeding memory limits" in t or "oomkilled" in t
         or "physical memory used" in t),
        ("driver", has_oom and any(k in t for k in ("collect", "topandas", ".take("))),
        ("skew", has_oom),
    ]
    for name, matched in rules:
        if matched:
            return dict(_DIAG[name])
    raise ValueError(log)

"""SPK-08 / 01-diagnose-oom — reference solution.

Classify an OOM/spill log excerpt by its signature.
"""


def diagnose(log):
    text = log.lower()

    # Spill: execution memory pressure, not a crash. Check first (it has its own
    # distinctive phrase and may co-occur with 'memory').
    if "spilling in-memory" in text or "spilling sort data" in text:
        return {
            "location": "executor",
            "cause": "spill",
            "fix": "more_partitions_or_memory",
        }

    # Container / overhead kill (off-heap / memory overhead).
    if "exceeding memory limits" in text or "oomkilled" in text or \
            "physical memory used" in text:
        return {
            "location": "executor",
            "cause": "overhead",
            "fix": "raise_overhead",
        }

    # Heap OOM from pulling data to the driver.
    if "outofmemoryerror" in text and (
        "collect" in text or "topandas" in text or ".take(" in text
    ):
        return {
            "location": "driver",
            "cause": "collect_to_driver",
            "fix": "avoid_collect",
        }

    # Executor OOM during a shuffle with a dominant task => skew.
    if "outofmemoryerror" in text:
        return {
            "location": "executor",
            "cause": "skew",
            "fix": "salt_or_aqe_skew",
        }

    raise ValueError(f"Unrecognized log excerpt: {log!r}")

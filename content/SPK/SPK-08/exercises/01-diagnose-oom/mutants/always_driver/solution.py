"""Mutant: always diagnoses a driver collect OOM — fails every executor/spill/overhead case."""


def diagnose(log):
    return {"location": "driver", "cause": "collect_to_driver", "fix": "avoid_collect"}

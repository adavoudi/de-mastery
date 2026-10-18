"""STH-01/01 — classify a data architecture by its storage + pipeline shape.

Implement `classify` so it returns exactly one of 'lambda', 'kappa',
'lakehouse', 'streamhouse' by applying the rubric in README.md IN PRIORITY ORDER.

The starter below is deliberately incomplete: it ignores precedence and the
dual-read streamhouse rule, so it misclassifies several architectures. Fix it.
"""


def classify(signals: dict) -> str:
    # TODO: apply the rubric in priority order (Lambda, streamhouse, lakehouse,
    # kappa) and raise ValueError when nothing matches.
    if signals.get("transactional_lake_table"):
        return "lakehouse"
    return "kappa"

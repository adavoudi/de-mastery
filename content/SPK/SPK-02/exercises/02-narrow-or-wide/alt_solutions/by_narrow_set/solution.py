"""SPK-02 / 02-narrow-or-wide — alt: enumerate the narrow ops, invert."""

_NARROW = {"filter", "select", "withColumn", "drop", "union", "coalesce"}


def is_wide(op):
    return not (op in _NARROW)

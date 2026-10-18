"""Mutant: thinks distinct is narrow (dedup needs a global shuffle)."""

_WIDE = {"groupBy_agg", "orderBy", "repartition"}


def is_wide(op):
    return op in _WIDE

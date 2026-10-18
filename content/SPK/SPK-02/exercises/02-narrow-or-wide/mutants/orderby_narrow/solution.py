"""Mutant: thinks orderBy is narrow (sort looks local, but a global sort shuffles)."""

_WIDE = {"groupBy_agg", "distinct", "repartition"}


def is_wide(op):
    return op in _WIDE

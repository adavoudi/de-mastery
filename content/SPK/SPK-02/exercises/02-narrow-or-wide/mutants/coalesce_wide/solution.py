"""Mutant: thinks coalesce is wide (confuses it with repartition)."""

_WIDE = {"groupBy_agg", "distinct", "orderBy", "repartition", "coalesce"}


def is_wide(op):
    return op in _WIDE

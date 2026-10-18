"""SPK-02 / 02-narrow-or-wide — reference solution."""

_WIDE = {"groupBy_agg", "distinct", "orderBy", "repartition"}


def is_wide(op):
    return op in _WIDE

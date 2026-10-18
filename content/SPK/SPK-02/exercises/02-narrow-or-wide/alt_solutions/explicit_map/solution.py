"""SPK-02 / 02-narrow-or-wide — alt: explicit per-op mapping."""

_SHUFFLES = {
    "filter": False,
    "select": False,
    "withColumn": False,
    "drop": False,
    "union": False,
    "coalesce": False,
    "groupBy_agg": True,
    "distinct": True,
    "orderBy": True,
    "repartition": True,
}


def is_wide(op):
    return _SHUFFLES[op]

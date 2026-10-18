"""Mutant: keeps the empty string instead of mapping blank -> NULL."""

from pyspark.sql import functions as F

_VALID = ["USD", "EUR", "GBP", "JPY"]


def clean_currency(orders):
    canon = F.upper(F.trim(F.col("currency")))  # WRONG: '' stays '' (no nullif)
    return orders.select(
        "order_id",
        canon.alias("currency_clean"),
        F.coalesce(canon.isin(_VALID), F.lit(False)).alias("is_valid"),
    )

"""Mutant: lets is_valid be NULL for blank codes instead of false."""

from pyspark.sql import functions as F

_VALID = ["USD", "EUR", "GBP", "JPY"]


def clean_currency(orders):
    canon = F.nullif(F.upper(F.trim(F.col("currency"))), F.lit(""))
    return orders.select(
        "order_id",
        canon.alias("currency_clean"),
        canon.isin(_VALID).alias("is_valid"),  # WRONG: NULL (not false) when canon is NULL
    )

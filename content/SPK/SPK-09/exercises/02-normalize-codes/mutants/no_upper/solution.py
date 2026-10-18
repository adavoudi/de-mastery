"""Mutant: forgets to upper-case, so 'usd' is not canonicalized to 'USD'."""

from pyspark.sql import functions as F

_VALID = ["USD", "EUR", "GBP", "JPY"]


def clean_currency(orders):
    canon = F.nullif(F.trim(F.col("currency")), F.lit(""))  # WRONG: no upper()
    return orders.select(
        "order_id",
        canon.alias("currency_clean"),
        F.coalesce(canon.isin(_VALID), F.lit(False)).alias("is_valid"),
    )

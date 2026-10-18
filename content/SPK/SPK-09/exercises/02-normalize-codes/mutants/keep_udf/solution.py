"""Mutant: keeps the per-row Python UDFs."""

from pyspark.sql.functions import udf

_VALID = {"USD", "EUR", "GBP", "JPY"}


def clean_currency(orders):
    def canon(code):
        if code is None:
            return None
        c = code.strip().upper()
        return None if c == "" else c

    canon_udf = udf(canon, "string")
    valid_udf = udf(lambda c: canon(c) in _VALID, "boolean")
    return orders.select(
        "order_id",
        canon_udf("currency").alias("currency_clean"),
        valid_udf("currency").alias("is_valid"),
    )

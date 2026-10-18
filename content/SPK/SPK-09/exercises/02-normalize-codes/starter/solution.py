"""SPK-09 / 02-normalize-codes — starter.

Works, but cleans the currency with a per-row Python UDF -> BatchEvalPython. Rewrite
so the plan has no BatchEvalPython (see README.md).
"""

from pyspark.sql import functions as F
from pyspark.sql.functions import udf

_VALID = {"USD", "EUR", "GBP", "JPY"}


def clean_currency(orders):
    def canon(code):
        if code is None:
            return None
        c = code.strip().upper()
        return None if c == "" else c

    def valid(code):
        c = canon(code)
        return c in _VALID

    canon_udf = udf(canon, "string")
    valid_udf = udf(valid, "boolean")
    return orders.select(
        "order_id",
        canon_udf("currency").alias("currency_clean"),
        valid_udf("currency").alias("is_valid"),
    )

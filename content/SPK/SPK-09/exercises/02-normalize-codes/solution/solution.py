"""SPK-09 / 02-normalize-codes — reference solution (native functions).

No Python UDF: upper/trim canonicalize, nullif turns blanks into NULL, isin judges
validity. The plan has no BatchEvalPython.
"""

from pyspark.sql import functions as F

_VALID = ["USD", "EUR", "GBP", "JPY"]


def clean_currency(orders):
    canon = F.nullif(F.upper(F.trim(F.col("currency"))), F.lit(""))
    return orders.select(
        "order_id",
        canon.alias("currency_clean"),
        F.coalesce(canon.isin(_VALID), F.lit(False)).alias("is_valid"),
    )

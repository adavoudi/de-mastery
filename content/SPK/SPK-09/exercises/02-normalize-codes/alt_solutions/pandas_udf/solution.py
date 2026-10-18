"""SPK-09 / 02-normalize-codes — alternate: vectorized pandas UDFs.

Each UDF receives a whole pandas.Series per Arrow batch (ArrowEvalPython), not a
scalar per row (BatchEvalPython), so the plan gate passes.
"""

import pandas as pd
from pyspark.sql.functions import pandas_udf

_VALID = {"USD", "EUR", "GBP", "JPY"}


@pandas_udf("string")
def _canon(code: pd.Series) -> pd.Series:
    s = code.str.strip().str.upper()
    return s.where(s != "", other=None)


@pandas_udf("boolean")
def _is_valid(code: pd.Series) -> pd.Series:
    s = code.str.strip().str.upper()
    return s.isin(_VALID).fillna(False)


def clean_currency(orders):
    return orders.select(
        "order_id",
        _canon("currency").alias("currency_clean"),
        _is_valid("currency").alias("is_valid"),
    )

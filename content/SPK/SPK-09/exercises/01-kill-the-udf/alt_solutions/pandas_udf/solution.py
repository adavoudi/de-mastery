"""SPK-09 / 01-kill-the-udf — alternate: vectorized pandas UDF for the arithmetic.

The FX rate is still joined (declaratively), but the final multiply is done with a
pandas UDF (Series -> Series). A pandas UDF crosses the JVM/Python boundary in Arrow
batches and appears as ArrowEvalPython, NOT BatchEvalPython — so it passes the
"no per-row Python round trip" gate while still being genuinely different code.
"""

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf


@pandas_udf("double")
def _mul(amount: pd.Series, rate: pd.Series) -> pd.Series:
    return amount * rate


def to_usd(payments, fx_rates):
    p = payments.withColumn(
        "rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string")
    )
    joined = p.join(F.broadcast(fx_rates), on=["currency", "rate_date"], how="left")
    return joined.select(
        "payment_id",
        "currency",
        "amount",
        _mul(F.col("amount"), F.col("usd_per_unit")).alias("usd_amount"),
    )

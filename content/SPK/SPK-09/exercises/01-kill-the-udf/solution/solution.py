"""SPK-09 / 01-kill-the-udf — reference solution (native join + arithmetic).

No Python UDF: the FX lookup is a left join to fx_rates and the conversion is plain
Column arithmetic, so the plan has no BatchEvalPython and stays fully optimizable.
"""

from pyspark.sql import functions as F


def to_usd(payments, fx_rates):
    p = payments.withColumn(
        "rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string")
    )
    joined = p.join(
        F.broadcast(fx_rates),
        on=["currency", "rate_date"],
        how="left",
    )
    return joined.select(
        "payment_id",
        "currency",
        "amount",
        (F.col("amount") * F.col("usd_per_unit")).alias("usd_amount"),
    )

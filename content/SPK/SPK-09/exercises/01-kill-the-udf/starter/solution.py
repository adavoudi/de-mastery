"""SPK-09 / 01-kill-the-udf — starter.

This version WORKS numerically but is slow: it looks up the FX rate with a plain
Python UDF, which the planner runs as a BatchEvalPython node (a per-row round trip
to a Python worker). See README.md. Rewrite it so the plan has no BatchEvalPython.
"""

from pyspark.sql import functions as F
from pyspark.sql.functions import udf


def to_usd(payments, fx_rates):
    # Build a {(rate_date, currency): usd_per_unit} lookup on the driver...
    rate_rows = fx_rates.collect()
    rate_map = {(r["rate_date"], r["currency"]): r["usd_per_unit"] for r in rate_rows}

    # ...and apply it per row with a Python UDF -> BatchEvalPython (slow, an optimizer fence).
    def convert(amount, currency, date_str):
        if amount is None:
            return None
        rate = rate_map.get((date_str, currency))
        if rate is None:
            return None
        return amount * rate

    convert_udf = udf(convert, "double")

    return (
        payments
        .withColumn("rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string"))
        .withColumn("usd_amount", convert_udf("amount", "currency", "rate_date"))
        .select("payment_id", "currency", "amount", "usd_amount")
    )

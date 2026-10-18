"""Mutant: keeps the per-row Python UDF (correct numbers, wrong plan)."""

from pyspark.sql import functions as F
from pyspark.sql.functions import udf


def to_usd(payments, fx_rates):
    rate_map = {(r["rate_date"], r["currency"]): r["usd_per_unit"]
                for r in fx_rates.collect()}

    def convert(amount, currency, date_str):
        if amount is None:
            return None
        rate = rate_map.get((date_str, currency))
        return None if rate is None else amount * rate

    convert_udf = udf(convert, "double")
    return (
        payments
        .withColumn("rate_date", F.to_date(F.to_timestamp("paid_ts_utc")).cast("string"))
        .withColumn("usd_amount", convert_udf("amount", "currency", "rate_date"))
        .select("payment_id", "currency", "amount", "usd_amount")
    )

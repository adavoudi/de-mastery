"""Mutant: filter via a Python UDF. Data is correct, but the predicate is opaque
to Catalyst so PushedFilters is empty — the pushdown assertion must fail."""

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def usd_orders(df):
    is_usd = F.udf(lambda c: c == "USD", BooleanType())
    return df.filter(is_usd(F.col("currency"))).select("order_id", "currency")

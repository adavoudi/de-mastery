"""Mutant: inclusive-of-NULL filter via != 'EUR'. Keeps GBP/JPY/XXX/'usd'/NULL
rows too, so the data oracle (strict currency = 'USD') fails."""

from pyspark.sql import functions as F


def usd_orders(df):
    return df.filter(F.col("currency") != "EUR").select("order_id", "currency")

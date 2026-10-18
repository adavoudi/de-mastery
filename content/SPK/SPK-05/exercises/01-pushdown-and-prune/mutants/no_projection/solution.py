"""Mutant: forgets to project. Filter pushes down, but every column is read
(wide ReadSchema) and the output has 7 columns, not 2."""

from pyspark.sql import functions as F


def usd_orders(df):
    return df.filter(F.col("currency") == "USD")

"""SPK-03 / 01-clean-customers — alt: na.drop + groupBy().agg(first)."""

from pyspark.sql import functions as F


def clean_customers(df):
    valid = df.na.drop(subset=["email"])
    # Collapse to one row per business key by taking the first email per key.
    return (
        valid.groupBy("company_id", "customer_id")
             .agg(F.first("email").alias("email"))
    )

"""Mutant: dedups on customer_id alone, merging different people across companies."""

from pyspark.sql import functions as F


def clean_customers(df):
    return (
        df.filter(F.col("email").isNotNull())
          .dropDuplicates(["customer_id"])  # WRONG: ignores company_id
          .select("company_id", "customer_id", "email")
    )

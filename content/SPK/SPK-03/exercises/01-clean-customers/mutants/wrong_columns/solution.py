"""Mutant: returns the wrong columns (keeps the whole row, no projection)."""

from pyspark.sql import functions as F


def clean_customers(df):
    # WRONG: does the filter and dedup but returns ALL columns, so the result schema
    # has extra fields (first_name, last_name, signup_date) the task did not ask for.
    return (
        df.filter(F.col("email").isNotNull())
          .dropDuplicates(["company_id", "customer_id"])
    )

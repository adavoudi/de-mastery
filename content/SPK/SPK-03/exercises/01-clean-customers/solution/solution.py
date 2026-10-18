"""SPK-03 / 01-clean-customers — reference solution (DataFrame API)."""

from pyspark.sql import functions as F


def clean_customers(df):
    return (
        df.filter(F.col("email").isNotNull())
          .dropDuplicates(["company_id", "customer_id"])
          .select("company_id", "customer_id", "email")
    )

"""Mutant: customer_id typed as STRING instead of LONG."""

from pyspark.sql.types import StringType, StructField, StructType

_SCHEMA = StructType([
    StructField("company_id", StringType()),
    StructField("customer_id", StringType()),  # WRONG: should be LongType
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("signup_date", StringType()),
])


def read_customers_csv(spark, csv_path):
    return (
        spark.read.schema(_SCHEMA)
             .option("header", True).option("nullValue", "").csv(csv_path)
    )

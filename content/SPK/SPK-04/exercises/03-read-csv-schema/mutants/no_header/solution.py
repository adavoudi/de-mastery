"""Mutant: forgets header=true, so the header line is treated as a data row."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

_SCHEMA = StructType([
    StructField("company_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("signup_date", StringType()),
])


def read_customers_csv(spark, csv_path):
    # WRONG: no header option -> the "company_id,customer_id,..." line becomes a row.
    return spark.read.schema(_SCHEMA).option("nullValue", "").csv(csv_path)

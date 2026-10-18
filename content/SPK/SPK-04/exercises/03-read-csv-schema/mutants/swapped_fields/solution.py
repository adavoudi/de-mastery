"""Mutant: swaps the first_name and last_name positions in the schema.

CSV columns are matched by position, so naming position 3 'last_name' and position 4
'first_name' loads each person's first name into last_name and vice versa.
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

_SCHEMA = StructType([
    StructField("company_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("last_name", StringType()),   # WRONG: position 3 is first_name
    StructField("first_name", StringType()),  # WRONG: position 4 is last_name
    StructField("email", StringType()),
    StructField("signup_date", StringType()),
])


def read_customers_csv(spark, csv_path):
    return (
        spark.read.schema(_SCHEMA)
             .option("header", True).option("nullValue", "").csv(csv_path)
    )

"""SPK-04 / 03-read-csv-schema — reference solution."""

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
    return (
        spark.read
             .schema(_SCHEMA)
             .option("header", True)
             .option("nullValue", "")
             .csv(csv_path)
    )

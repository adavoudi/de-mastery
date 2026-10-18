"""SPK-04 / 03-read-csv-schema — alt: generic format/load with a StructType."""

from pyspark.sql.types import LongType, StringType, StructField, StructType


def _schema():
    s = ["company_id", "first_name", "last_name", "email", "signup_date"]
    fields = [StructField("company_id", StringType()),
              StructField("customer_id", LongType())]
    fields += [StructField(n, StringType()) for n in
               ("first_name", "last_name", "email", "signup_date")]
    return StructType(fields)


def read_customers_csv(spark, csv_path):
    return (
        spark.read
             .format("csv")
             .schema(_schema())
             .option("header", "true")
             .option("nullValue", "")
             .load(csv_path)
    )

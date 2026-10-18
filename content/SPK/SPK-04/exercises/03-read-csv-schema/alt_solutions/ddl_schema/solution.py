"""SPK-04 / 03-read-csv-schema — alt: DDL-string schema instead of StructType."""

_DDL = ("company_id STRING, customer_id BIGINT, first_name STRING, "
        "last_name STRING, email STRING, signup_date STRING")


def read_customers_csv(spark, csv_path):
    return (
        spark.read
             .schema(_DDL)
             .options(header=True, nullValue="")
             .csv(csv_path)
    )

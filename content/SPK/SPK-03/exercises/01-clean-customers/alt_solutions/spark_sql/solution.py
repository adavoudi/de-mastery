"""SPK-03 / 01-clean-customers — alt: Spark SQL on a temp view."""


def clean_customers(df):
    df.sparkSession.catalog.dropTempView("raw_customers")
    df.createOrReplaceTempView("raw_customers")
    return df.sparkSession.sql(
        """
        SELECT DISTINCT company_id, customer_id, email
        FROM raw_customers
        WHERE email IS NOT NULL
        """
    )

"""SPK-03 / 03-payment-summary — alt: Spark SQL."""


def payment_summary(df):
    df.createOrReplaceTempView("pay")
    return df.sparkSession.sql(
        """
        SELECT
          company_id,
          count(*)                     AS n_payments,
          count(amount)                AS n_amounts,
          coalesce(sum(amount), 0.0)   AS total_amount
        FROM pay
        GROUP BY company_id
        """
    )

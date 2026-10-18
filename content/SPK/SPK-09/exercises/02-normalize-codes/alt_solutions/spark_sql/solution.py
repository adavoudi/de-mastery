"""SPK-09 / 02-normalize-codes — alternate: Spark SQL."""


def clean_currency(orders):
    spark = orders.sparkSession
    orders.createOrReplaceTempView("o_spk09_02")
    return spark.sql(
        """
        SELECT order_id,
               nullif(upper(trim(currency)), '') AS currency_clean,
               coalesce(nullif(upper(trim(currency)), '')
                        IN ('USD','EUR','GBP','JPY'), false) AS is_valid
        FROM o_spk09_02
        """
    )

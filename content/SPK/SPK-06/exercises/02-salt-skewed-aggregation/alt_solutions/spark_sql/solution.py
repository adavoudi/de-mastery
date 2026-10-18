"""SPK-06 / 02 — alt: Spark SQL two-phase salted aggregation."""


def salted_totals(order_items, num_salts):
    order_items.createOrReplaceTempView("oi")
    return order_items.sparkSession.sql(
        f"""
        SELECT company_id, SUM(partial) AS total
        FROM (
            SELECT company_id, pmod(hash(order_item_id), {num_salts}) AS salt,
                   SUM(line_amount) AS partial
            FROM oi
            GROUP BY company_id, pmod(hash(order_item_id), {num_salts})
        )
        GROUP BY company_id
        """
    )

"""SPK-05 / 02 — alt: Spark SQL. Same optimized plan: Catalyst pushes the WHERE
below the join into the orders scan."""


def usd_line_items(orders, order_items):
    orders.createOrReplaceTempView("o")
    order_items.createOrReplaceTempView("oi")
    return orders.sparkSession.sql(
        """
        SELECT o.order_id, oi.order_item_id, oi.line_amount
        FROM o JOIN oi
          ON o.company_id = oi.company_id AND o.order_id = oi.order_id
        WHERE o.currency = 'USD'
        """
    )

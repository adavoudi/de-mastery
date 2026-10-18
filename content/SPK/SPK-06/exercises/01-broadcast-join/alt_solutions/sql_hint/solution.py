"""SPK-06 / 01 — alt: Spark SQL with a BROADCAST hint on the small side."""


def enrich_orders(orders, companies):
    orders.createOrReplaceTempView("o")
    companies.createOrReplaceTempView("c_all")
    return orders.sparkSession.sql(
        """
        SELECT /*+ BROADCAST(c) */ o.order_id, o.company_id, c.name
        FROM o
        LEFT JOIN (SELECT company_id, name FROM c_all WHERE acquired_by IS NULL) c
          ON o.company_id = c.company_id
        """
    )

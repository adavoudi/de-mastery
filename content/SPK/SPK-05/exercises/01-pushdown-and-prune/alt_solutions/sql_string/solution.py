"""SPK-05 / 01 — alt: SQL-string filter + select, same pushdown/pruning."""


def usd_orders(df):
    return df.where("currency = 'USD'").select("order_id", "currency")

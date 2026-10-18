"""Mutant: forgets the currency filter entirely — returns line items for ALL
currencies, so the row set is a strict superset of the USD-only oracle."""


def usd_line_items(orders, order_items):
    return (
        orders.join(order_items, ["company_id", "order_id"], "inner")
        .select("order_id", "order_item_id", "line_amount")
    )

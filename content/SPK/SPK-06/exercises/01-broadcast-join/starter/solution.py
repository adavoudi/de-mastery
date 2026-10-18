"""SPK-06 / 01-broadcast-join — starter. See README.md."""


def enrich_orders(orders, companies):
    # TODO: restrict companies to parents (acquired_by IS NULL), then LEFT-join
    # orders to it on company_id, broadcasting the SMALL side explicitly
    # (auto-broadcast is disabled in the test). Return order_id, company_id, name.
    raise NotImplementedError

"""SPK-06 / 02-salt-skewed-aggregation — starter. See README.md."""


def salted_totals(order_items, num_salts):
    # TODO: two-phase salted aggregation.
    #   1) add salt in [0, num_salts)
    #   2) partial sum by (company_id, salt)
    #   3) final sum by company_id -> one row per company, columns company_id, total
    raise NotImplementedError

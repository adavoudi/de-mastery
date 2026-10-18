"""SPK-06 / 03-broadcast-var-accumulator — starter. See README.md."""


def validate_currencies(spark, orders, valid_currencies):
    # TODO: broadcast valid_currencies; iterate orders.rdd; increment an
    # accumulator for rows whose currency is NOT in the allowlist; trigger an
    # action; return (valid_df, invalid_count).
    raise NotImplementedError

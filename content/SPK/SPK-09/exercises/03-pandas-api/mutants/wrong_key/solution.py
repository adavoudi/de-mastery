"""Mutant: groups by order_id instead of company_id (wrong grain)."""

import pyspark.pandas as ps

ps.set_option("compute.fail_on_ansi_mode", False)


def line_summary(order_items):
    psdf = order_items.pandas_api()
    agg = (
        psdf.groupby("order_id")  # WRONG grain
        .agg(n_lines=("order_item_id", "count"),
             total_line_amount=("line_amount", "sum"))
        .reset_index()
    )
    # Rename to look right, but the grain is wrong.
    return agg.rename(columns={"order_id": "company_id"}).to_spark()

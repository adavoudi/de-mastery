"""Mutant: total_line_amount uses mean instead of sum."""

import pyspark.pandas as ps

ps.set_option("compute.fail_on_ansi_mode", False)


def line_summary(order_items):
    psdf = order_items.pandas_api()
    agg = (
        psdf.groupby("company_id")
        .agg(n_lines=("order_item_id", "count"),
             total_line_amount=("line_amount", "mean"))  # WRONG: mean, not sum
        .reset_index()
    )
    return agg.to_spark()

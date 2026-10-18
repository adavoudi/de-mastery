"""Mutant: n_lines counts distinct order_ids instead of rows."""

import pyspark.pandas as ps

ps.set_option("compute.fail_on_ansi_mode", False)


def line_summary(order_items):
    psdf = order_items.pandas_api()
    agg = (
        psdf.groupby("company_id")
        .agg(n_lines=("order_id", "nunique"),  # WRONG: distinct orders, not line rows
             total_line_amount=("line_amount", "sum"))
        .reset_index()
    )
    return agg.to_spark()

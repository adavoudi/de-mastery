"""SPK-09 / 03-pandas-api — alternate: compute the two aggregates separately and merge.

Still Pandas API on Spark, but using groupby().size() and groupby()[col].sum()
separately, then a pandas-style merge — a different idiom from a single named agg.
"""

import pyspark.pandas as ps

ps.set_option("compute.fail_on_ansi_mode", False)


def line_summary(order_items):
    psdf = order_items.pandas_api()
    n = psdf.groupby("company_id").size().rename("n_lines").reset_index()
    tot = (psdf.groupby("company_id")["line_amount"].sum()
           .rename("total_line_amount").reset_index())
    merged = n.merge(tot, on="company_id")
    return merged.to_spark()

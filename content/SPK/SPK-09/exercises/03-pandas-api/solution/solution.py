"""SPK-09 / 03-pandas-api — reference solution (Pandas API on Spark).

Uses df.pandas_api() + a pandas-style groupby.agg + .to_spark(), so the work runs
distributed/lazily on Spark rather than being collected to the driver.
"""

import pyspark.pandas as ps

# Spark 4.0 enables ANSI mode by default, which the Pandas API refuses to run under
# unless told; opt in explicitly (the shared test session leaves ANSI on).
ps.set_option("compute.fail_on_ansi_mode", False)


def line_summary(order_items):
    psdf = order_items.pandas_api()
    agg = (
        psdf.groupby("company_id")
        .agg(n_lines=("order_item_id", "count"),
             total_line_amount=("line_amount", "sum"))
        .reset_index()
    )
    return agg.to_spark()

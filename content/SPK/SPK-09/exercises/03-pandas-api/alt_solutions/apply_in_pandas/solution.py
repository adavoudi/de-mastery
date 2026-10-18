"""SPK-09 / 03-pandas-api — alternate: grouped-map applyInPandas.

A different vectorized Python route: each company's rows arrive as a whole
pandas.DataFrame and we return a one-row pandas.DataFrame summary. This is the
function-API cousin of the Pandas API on Spark and also avoids a driver collect.
"""

import pandas as pd

_OUT_SCHEMA = "company_id string, n_lines long, total_line_amount double"


def line_summary(order_items):
    def summarize(pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({
            "company_id": [pdf["company_id"].iloc[0]],
            "n_lines": [len(pdf)],
            "total_line_amount": [pdf["line_amount"].sum()],
        })

    return order_items.groupBy("company_id").applyInPandas(summarize, _OUT_SCHEMA)

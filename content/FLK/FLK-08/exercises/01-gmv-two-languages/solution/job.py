import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def parse_orders(spec):
    rows = []
    if spec:
        for rec in spec.split(";"):
            ccy, amt = rec.split(",")
            rows.append((ccy, int(amt)))
    return rows


def main():
    orders = parse_orders(os.environ.get("ORDERS", ""))
    t = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    # Build a VALUES-based query so the pipeline is self-contained and
    # deterministic. (from_elements would work equally well.)
    if orders:
        values = ", ".join(f"('{ccy}', CAST({amt} AS BIGINT))" for ccy, amt in orders)
        query = (f"SELECT ccy, SUM(amt) AS gmv FROM (VALUES {values}) "
                 f"AS t(ccy, amt) GROUP BY ccy")
    else:
        query = "SELECT CAST('x' AS STRING) AS ccy, CAST(0 AS BIGINT) AS gmv WHERE 1=0"

    totals = {}
    for row in t.execute_sql(query).collect():
        totals[row[0]] = int(row[1])

    print("DE_RESULT=" + json.dumps(totals, sort_keys=True))


if __name__ == "__main__":
    main()

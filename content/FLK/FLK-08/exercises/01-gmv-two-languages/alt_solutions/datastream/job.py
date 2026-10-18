import json
import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col


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
    schema = DataTypes.ROW([
        DataTypes.FIELD("ccy", DataTypes.STRING()),
        DataTypes.FIELD("amt", DataTypes.BIGINT()),
    ])
    tbl = t.from_elements(orders, schema) if orders else t.from_elements(
        [("x", 0)], schema).filter(col("amt") < 0)
    agg = tbl.group_by(col("ccy")).select(col("ccy"), col("amt").sum.alias("gmv"))
    totals = {}
    for row in agg.execute().collect():
        totals[row[0]] = int(row[1])
    print("DE_RESULT=" + json.dumps(totals, sort_keys=True))


if __name__ == "__main__":
    main()

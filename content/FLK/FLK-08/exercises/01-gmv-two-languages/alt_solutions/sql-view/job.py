import json
import os

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment


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
    src = t.from_elements(orders if orders else [("x", 0)], schema)
    t.create_temporary_view("orders", src)
    where = "" if orders else " WHERE amt < 0"
    totals = {}
    for row in t.execute_sql(
            f"SELECT ccy, SUM(amt) FROM orders{where} GROUP BY ccy").collect():
        totals[row[0]] = int(row[1])
    print("DE_RESULT=" + json.dumps(totals, sort_keys=True))


if __name__ == "__main__":
    main()

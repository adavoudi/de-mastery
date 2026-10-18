import json

# Starter: not implemented. Prints an empty result so the PyFlink test fails (G1).
#
# TODO: read ORDERS from the environment, build a PyFlink TableEnvironment in
# batch mode, run a per-currency SUM(amt) GROUP BY ccy, and print
# "DE_RESULT=" + json.dumps(totals, sort_keys=True).


def main():
    totals = {}
    print("DE_RESULT=" + json.dumps(totals, sort_keys=True))


if __name__ == "__main__":
    main()

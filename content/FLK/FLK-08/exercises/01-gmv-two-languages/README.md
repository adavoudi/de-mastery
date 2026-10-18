# Per-currency GMV in Java Table API and PyFlink

Write the **same** pipeline twice — once in the **Java Table API** and once in **PyFlink** — and
prove both compute the same answer. The pipeline is a per-currency gross-merchandise-value (GMV):
group orders by currency and sum the amount. It is the streaming analogue of a dbt SQL model and the
equivalent dbt Python model.

### Java — `de.flk.GmvTable`

```java
public static Map<String, Long> run(List<String[]> orders) throws Exception
```

- `orders` rows are `{ccy, amtMinor}` (amount in minor units as a decimal string).
- Build a `TableEnvironment` in batch mode, make a table from the rows, group by `ccy` and sum
  `amtMinor`, and return `ccy -> total` as a `Map<String,Long>`. You may use the Table API
  (`groupBy(...).select(...)`) or `executeSql("SELECT ccy, SUM(amt) ... GROUP BY ccy")` — the grader
  checks the numbers, not the API.

### PyFlink — `job.py`

Read the same orders from the environment variable `ORDERS` (format: `ccy,amt;ccy,amt;...`), run the
same per-currency GMV with PyFlink's Table API (or `execute_sql`), and print exactly one line:

```
DE_RESULT={"EUR": 1250000, "USD": 500000}
```

i.e. `print("DE_RESULT=" + json.dumps(totals, sort_keys=True))` where `totals` maps each currency to
the integer sum of `amtMinor`.

### Grading

The grader runs the Java method (via the embedded Flink runner) and the PyFlink script (in its own
venv) over the **same** orders and compares each to one independent Python oracle (`SUM(amt) GROUP BY
ccy`). Both must match. The PyFlink test **skips cleanly** if the PyFlink environment is not
installed, but the Java Table API test always grades.

### Why the starter fails

The Java method returns an empty map and `job.py` prints nothing useful. Implement both so each
reproduces the per-currency totals.

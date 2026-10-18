# Build a customer golden record from 3 streams with Paimon partial-update

Three source systems each know *part* of a customer:

- **CRM** knows `name`
- **billing** knows `email`
- **loyalty** knows `phone`

Each system writes a row keyed by `customer_id`. It fills only its own column and leaves the others
`NULL`. Your job is to build the Paimon table and the insert statements for each source. The three
partial rows must **merge into one complete golden record** for each customer. This is continuous
MDM in the storage layer.

Implement, in `solution.py`:

```python
def table_ddl(table: str) -> str:
    """Return the CREATE TABLE statement for the golden-record table `table`
    with columns (customer_id INT, name STRING, email STRING, phone STRING)."""

def source_inserts(table: str, crm, billing, loyalty) -> list[str]:
    """Return INSERT statements that write each source's rows into `table`,
    filling only that source's column(s) and NULL for the rest."""
```

The inputs to `source_inserts` are lists of tuples:

- `crm`: `[(customer_id, name), ...]`
- `billing`: `[(customer_id, email), ...]`
- `loyalty`: `[(customer_id, phone), ...]`

### Requirements

- The table must use the **`partial-update`** merge engine, a **`PRIMARY KEY(customer_id) NOT
  ENFORCED`**, and **`'bucket'='1'`** (deterministic layout).
- Each `INSERT` fills only the column(s) of the source that owns them. The other string columns are
  `CAST(NULL AS STRING)`.
- `customer_id` is an `INT`. `name`, `email` and `phone` are `STRING`.

### What the tests do (G6 oracle)

The test runs your DDL and inserts through the native embedded Paimon runner (`paimon_sql.batch`,
no Docker). Then it reads `SELECT * FROM <table> ORDER BY customer_id`. It compares the merged rows
with an **independent Python oracle**. The oracle computes the golden record in a different way
(latest non-null value per column). The test also checks these points:

- A customer known to all three sources gets all three fields (the merge really combines them).
- A later `NULL` from one source does **not** erase a value that another source set.
- The result is the same whether a customer appears in one, two or three sources.

The whole suite may only skip. This happens when the vendored Flink/JDK/Paimon jars are missing.
Then run `bash labs/fetch_streamhouse.sh` first. The fixture prints the exact command.

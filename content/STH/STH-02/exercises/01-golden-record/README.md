# Build a customer golden record from 3 streams with Paimon partial-update

Three source systems each know *part* of a customer:

- **CRM** knows `name`
- **billing** knows `email`
- **loyalty** knows `phone`

Each writes a row keyed by `customer_id` with only its own column filled and the others `NULL`. Your
job is to build the Paimon table and the per-source inserts so the three partial rows **merge into
one complete golden record** per customer — continuous MDM in the storage layer.

Implement, in `solution.py`:

```python
def table_ddl(table: str) -> str:
    """Return the CREATE TABLE statement for the golden-record table `table`
    with columns (customer_id INT, name STRING, email STRING, phone STRING)."""

def source_inserts(table: str, crm, billing, loyalty) -> list[str]:
    """Return INSERT statements that write each source's rows into `table`,
    filling only that source's column(s) and NULL for the rest."""
```

Inputs to `source_inserts` are lists of tuples:

- `crm`: `[(customer_id, name), ...]`
- `billing`: `[(customer_id, email), ...]`
- `loyalty`: `[(customer_id, phone), ...]`

### Requirements

- The table must use the **`partial-update`** merge engine, a **`PRIMARY KEY(customer_id) NOT
  ENFORCED`**, and **`'bucket'='1'`** (deterministic layout).
- Each `INSERT` fills only the owning source's column(s); the other string columns are
  `CAST(NULL AS STRING)`.
- `customer_id` is an `INT`; `name`, `email`, `phone` are `STRING`.

### What the tests do (G6 oracle)

The test runs your DDL + inserts through the native embedded Paimon runner (`paimon_sql.batch`, no
Docker) and reads `SELECT * FROM <table> ORDER BY customer_id`. It compares the merged rows against
an **independent Python oracle** that computes the golden record a different way (per-column latest
non-null). It also checks that:

- a customer known to all three sources gets all three fields (the merge actually combines);
- a later `NULL` from one source does **not** erase a value another source set;
- the result is identical whether a customer appears in one, two or three sources.

If the whole suite can only skip (the vendored Flink/JDK/Paimon jars are missing), run
`bash labs/fetch_streamhouse.sh` first — the fixture prints the exact command.

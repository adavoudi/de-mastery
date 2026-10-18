"""STH-02/01 — alt: column-list INSERT (unlisted columns default to NULL).

Instead of spelling out CAST(NULL AS STRING) for the columns a source does not
own, use Flink SQL's column-list INSERT so the engine fills the rest with NULL.
Same partial-update table; a different, more declarative insert style.
"""


def table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        "customer_id INT, name STRING, email STRING, phone STRING, "
        "PRIMARY KEY(customer_id) NOT ENFORCED) "
        "WITH ('merge-engine'='partial-update', 'bucket'='1')"
    )


def _s(v):
    return "CAST(NULL AS STRING)" if v is None else "'" + str(v).replace("'", "''") + "'"


def source_inserts(table, crm, billing, loyalty):
    stmts = []
    for cid, name in crm:
        stmts.append(f"INSERT INTO {table} (customer_id, name) VALUES ({cid}, {_s(name)})")
    for cid, email in billing:
        stmts.append(f"INSERT INTO {table} (customer_id, email) VALUES ({cid}, {_s(email)})")
    for cid, phone in loyalty:
        stmts.append(f"INSERT INTO {table} (customer_id, phone) VALUES ({cid}, {_s(phone)})")
    return stmts

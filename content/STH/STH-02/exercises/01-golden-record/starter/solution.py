"""STH-02/01 — build a customer golden record from 3 streams with Paimon.

TODO: pick the right merge engine so partial rows from CRM, billing and loyalty
COMBINE into one complete record. The starter below uses the DEFAULT engine,
which keeps the last whole record and loses the other sources' columns.
"""


def table_ddl(table: str) -> str:
    # BUG: no merge-engine set -> defaults to 'deduplicate' (last-writer-wins),
    # so each source's NULLs overwrite the others. Use 'partial-update'.
    return (
        f"CREATE TABLE {table} ("
        "customer_id INT, name STRING, email STRING, phone STRING, "
        "PRIMARY KEY(customer_id) NOT ENFORCED) "
        "WITH ('bucket'='1')"
    )


def _lit(v):
    return "CAST(NULL AS STRING)" if v is None else "'" + str(v).replace("'", "''") + "'"


def source_inserts(table, crm, billing, loyalty):
    stmts = []
    for cid, name in crm:
        stmts.append(
            f"INSERT INTO {table} VALUES "
            f"({cid}, {_lit(name)}, CAST(NULL AS STRING), CAST(NULL AS STRING))"
        )
    for cid, email in billing:
        stmts.append(
            f"INSERT INTO {table} VALUES "
            f"({cid}, CAST(NULL AS STRING), {_lit(email)}, CAST(NULL AS STRING))"
        )
    for cid, phone in loyalty:
        stmts.append(
            f"INSERT INTO {table} VALUES "
            f"({cid}, CAST(NULL AS STRING), CAST(NULL AS STRING), {_lit(phone)})"
        )
    return stmts

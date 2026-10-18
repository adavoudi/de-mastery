"""STH-02/01 — reference: Paimon partial-update customer golden record.

The three source systems each write a partial row keyed by customer_id; the
partial-update merge engine combines them into one complete row per customer.
"""


def table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        "customer_id INT, name STRING, email STRING, phone STRING, "
        "PRIMARY KEY(customer_id) NOT ENFORCED) "
        "WITH ('merge-engine'='partial-update', 'bucket'='1')"
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

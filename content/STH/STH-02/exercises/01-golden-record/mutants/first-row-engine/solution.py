"""MUTANT — first-row merge engine: keeps the FIRST record, ignores later ones."""


def table_ddl(table: str) -> str:
    # BUG: 'first-row' keeps the first-arriving record for a key and ignores all
    # later writers, so only the CRM row survives and email/phone stay NULL.
    return (
        f"CREATE TABLE {table} ("
        "customer_id INT, name STRING, email STRING, phone STRING, "
        "PRIMARY KEY(customer_id) NOT ENFORCED) "
        "WITH ('merge-engine'='first-row', 'bucket'='1')"
    )


def _s(v):
    return "CAST(NULL AS STRING)" if v is None else "'" + str(v).replace("'", "''") + "'"


def source_inserts(table, crm, billing, loyalty):
    stmts = []
    for cid, name in crm:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, {_s(name)}, CAST(NULL AS STRING), CAST(NULL AS STRING))")
    for cid, email in billing:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, CAST(NULL AS STRING), {_s(email)}, CAST(NULL AS STRING))")
    for cid, phone in loyalty:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, CAST(NULL AS STRING), CAST(NULL AS STRING), {_s(phone)})")
    return stmts

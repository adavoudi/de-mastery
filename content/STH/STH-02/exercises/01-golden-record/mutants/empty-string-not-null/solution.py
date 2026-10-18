"""MUTANT — writes '' (empty string) instead of NULL for columns a source does not own."""


def table_ddl(table: str) -> str:
    # Correct engine, but the inserts are wrong:
    return (
        f"CREATE TABLE {table} ("
        "customer_id INT, name STRING, email STRING, phone STRING, "
        "PRIMARY KEY(customer_id) NOT ENFORCED) "
        "WITH ('merge-engine'='partial-update', 'bucket'='1')"
    )


def _s(v):
    # BUG: empty string instead of SQL NULL for an unowned column. partial-update
    # treats '' as a non-null value, so a later source's '' OVERWRITES an earlier
    # real value -> fields get blanked out instead of combined.
    return "''" if v is None else "'" + str(v).replace("'", "''") + "'"


def source_inserts(table, crm, billing, loyalty):
    stmts = []
    for cid, name in crm:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, {_s(name)}, {_s(None)}, {_s(None)})")
    for cid, email in billing:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, {_s(None)}, {_s(email)}, {_s(None)})")
    for cid, phone in loyalty:
        stmts.append(f"INSERT INTO {table} VALUES ({cid}, {_s(None)}, {_s(None)}, {_s(phone)})")
    return stmts

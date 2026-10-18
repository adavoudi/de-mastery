"""STH-02/01 — alt: one batched multi-row INSERT per source (not row-by-row).

Same partial-update table; the inserts are built as a single
INSERT INTO ... VALUES (..),(..),(..) per source instead of one INSERT per row.
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


def _batch(table, rows):
    if not rows:
        return None
    values = ", ".join(rows)
    return f"INSERT INTO {table} VALUES {values}"


def source_inserts(table, crm, billing, loyalty):
    crm_rows = [f"({c}, {_s(n)}, CAST(NULL AS STRING), CAST(NULL AS STRING))" for c, n in crm]
    bill_rows = [f"({c}, CAST(NULL AS STRING), {_s(e)}, CAST(NULL AS STRING))" for c, e in billing]
    loy_rows = [f"({c}, CAST(NULL AS STRING), CAST(NULL AS STRING), {_s(p)})" for c, p in loyalty]
    out = []
    for stmt in (_batch(table, crm_rows), _batch(table, bill_rows), _batch(table, loy_rows)):
        if stmt is not None:
            out.append(stmt)
    return out

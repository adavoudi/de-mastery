"""STH-03/01 — reference: a tiered lake (Paimon PK) table whose streaming and
batch reads return the same final state.

The lake tier Fluss writes into is a Paimon PK table. A PK table with the
deduplicate merge engine collapses multiple writes per key to the latest value,
so a bounded streaming scan and a batch scan agree — the read-consistency
invariant of a tiered streamhouse table.
"""


def tier_table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT, PRIMARY KEY(id) NOT ENFORCED) "
        "WITH ('merge-engine'='deduplicate', 'bucket'='1', "
        "'changelog-producer'='input')"
    )


def upsert_inserts(table, upserts):
    stmts = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        stmts.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return stmts

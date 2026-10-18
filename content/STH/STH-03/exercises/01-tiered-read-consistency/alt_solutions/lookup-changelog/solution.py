"""STH-03/01 — alt: the 'lookup' changelog producer (true before/after images).

Same PK + deduplicate final state, but the changelog is produced by 'lookup'
instead of 'input'. The merged final state read in streaming and batch is
identical; a different, heavier changelog configuration.
"""


def tier_table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT, PRIMARY KEY(id) NOT ENFORCED) "
        "WITH ('merge-engine'='deduplicate', 'bucket'='1', "
        "'changelog-producer'='lookup')"
    )


def upsert_inserts(table, upserts):
    out = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        out.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return out

"""MUTANT — first-row merge engine: keeps the FIRST value per id, not the latest."""


def tier_table_ddl(table: str) -> str:
    # BUG: 'first-row' keeps the first write per key and ignores later upserts,
    # so the final balance is the ORIGINAL, not the latest -> wrong final state.
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT, PRIMARY KEY(id) NOT ENFORCED) "
        "WITH ('merge-engine'='first-row', 'bucket'='1', "
        "'changelog-producer'='input')"
    )


def upsert_inserts(table, upserts):
    out = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        out.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return out

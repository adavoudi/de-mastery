"""MUTANT — aggregation (sum) merge engine: sums balances instead of replacing."""


def tier_table_ddl(table: str) -> str:
    # BUG: the aggregation engine with sum ADDS every write for a key, so the
    # final bal is the sum of all upserts, not the latest value -> wrong state.
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT, PRIMARY KEY(id) NOT ENFORCED) "
        "WITH ('merge-engine'='aggregation', 'bucket'='1', "
        "'fields.bal.aggregate-function'='sum', "
        "'changelog-producer'='full-compaction')"
    )


def upsert_inserts(table, upserts):
    out = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        out.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return out

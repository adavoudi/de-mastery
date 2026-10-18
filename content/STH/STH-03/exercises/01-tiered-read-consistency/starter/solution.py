"""STH-03/01 — build the tiered lake table so streaming and batch reads match.

TODO: the starter below creates an APPEND-ONLY table (no primary key), so it
keeps every version of a key. A streaming scan then shows duplicate rows that a
batch scan has not collapsed -> the read-consistency invariant fails. Give the
table a primary key and a merge engine so the latest write per id wins.
"""


def tier_table_ddl(table: str) -> str:
    # BUG: no PRIMARY KEY and no merge engine -> append-only; every write is kept.
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT) "
        "WITH ('bucket'='1', 'bucket-key'='id')"
    )


def upsert_inserts(table, upserts):
    stmts = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        stmts.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return stmts

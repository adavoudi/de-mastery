"""MUTANT — append-only table (no primary key): keeps every version of a key."""


def tier_table_ddl(table: str) -> str:
    # BUG: no PRIMARY KEY -> append-only. Every upsert is retained, so a scan
    # returns multiple rows per id instead of the collapsed latest value.
    return f"CREATE TABLE {table} (id INT, bal INT) WITH ('bucket'='1', 'bucket-key'='id')"


def upsert_inserts(table, upserts):
    out = []
    for cid, bal in upserts:
        bal_lit = "CAST(NULL AS INT)" if bal is None else str(int(bal))
        out.append(f"INSERT INTO {table} VALUES ({int(cid)}, {bal_lit})")
    return out

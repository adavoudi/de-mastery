"""STH-03/01 — alt: full-compaction changelog + batched inserts.

Same PK deduplicate final state. The changelog is produced by full compaction
and the upserts are emitted as fewer, batched multi-row INSERTs — a different
write shape and changelog strategy, same read-consistent final state.
"""


def tier_table_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        "id INT, bal INT, PRIMARY KEY(id) NOT ENFORCED) "
        "WITH ('merge-engine'='deduplicate', 'bucket'='1', "
        "'changelog-producer'='full-compaction')"
    )


def _lit(bal):
    return "CAST(NULL AS INT)" if bal is None else str(int(bal))


def upsert_inserts(table, upserts):
    # Preserve order but coalesce consecutive runs into batched INSERTs. Each
    # INSERT still applies writes left-to-right, so the last value per id wins.
    out = []
    buf = []
    for cid, bal in upserts:
        buf.append(f"({int(cid)}, {_lit(bal)})")
        if len(buf) == 2:
            out.append(f"INSERT INTO {table} VALUES " + ", ".join(buf))
            buf = []
    if buf:
        out.append(f"INSERT INTO {table} VALUES " + ", ".join(buf))
    return out

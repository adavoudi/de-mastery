"""Starter — make this an UPSERT pipeline. The stubs below create an append-only
table and never carry the new column, so the table does not reconcile with the
source (G1 fails) until you add the primary key + format-version 2 and the
evolution. See README.
"""


def target_ddl(table: str) -> str:
    # TODO: make this upsert-capable (PRIMARY KEY + format-version 2).
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT)"


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def evolve_ddl(table: str) -> str:
    # TODO: ALTER TABLE ... ADD (region STRING)
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    # TODO: also carry the new column.
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"

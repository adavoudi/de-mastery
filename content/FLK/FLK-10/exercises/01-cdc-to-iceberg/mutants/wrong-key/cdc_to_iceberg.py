"""MUTANT: the primary key is declared on `name` instead of `id`, so updates and
deletes are applied against the wrong key and the table does not reflect the
source's per-id current state."""


def target_ddl(table: str) -> str:
    # BUG: wrong primary key column.
    return (
        f"CREATE TABLE {table} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (name) NOT ENFORCED"
        f") WITH ('format-version' = '2')"
    )


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def evolve_ddl(table: str) -> str:
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt, region FROM {src}"

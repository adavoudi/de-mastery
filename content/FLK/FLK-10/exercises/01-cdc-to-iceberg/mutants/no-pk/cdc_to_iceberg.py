"""MUTANT: target table is format-version 2 but has NO primary key, so Flink
cannot apply updates/deletes by key — it appends, and the table never reflects
the current state (deleted/updated keys linger)."""


def target_ddl(table: str) -> str:
    # BUG: no PRIMARY KEY -> not upsert-capable.
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT) WITH ('format-version' = '2')"


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def evolve_ddl(table: str) -> str:
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt, region FROM {src}"

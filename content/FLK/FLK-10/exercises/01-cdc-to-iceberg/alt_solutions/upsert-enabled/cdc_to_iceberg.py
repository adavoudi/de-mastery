"""Alt A — upsert-capable table declared with an explicit write.upsert.enabled
property, and the upsert written with an explicit column list in a different
order (same result)."""


def target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')"
    )


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} (id, name, amt) SELECT id, name, amt FROM {src}"


def evolve_ddl(table: str) -> str:
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    return (f"INSERT INTO {tgt} (id, name, amt, region) "
            f"SELECT id, name, amt, region FROM {src}")

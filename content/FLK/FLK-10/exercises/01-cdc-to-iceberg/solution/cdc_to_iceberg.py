"""Reference: SQL for a Debezium changelog -> upserted Iceberg table.

The test provides the catalog + debezium-json source and calls these to build
the upsert-capable target, apply the changelog, evolve the schema and apply the
phase-2 changelog.
"""


def target_ddl(table: str) -> str:
    # upsert-capable: primary key + Iceberg v2 (row-level deletes).
    return (
        f"CREATE TABLE {table} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('format-version' = '2')"
    )


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def evolve_ddl(table: str) -> str:
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt, region FROM {src}"

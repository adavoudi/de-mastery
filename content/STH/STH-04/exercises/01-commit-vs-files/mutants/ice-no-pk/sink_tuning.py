"""MUTANT: the Iceberg target has NO primary key, so Flink cannot route +U/-D by
key — it appends every change event. The deleted key (id=2) and the superseded
row (id=1 amt=100) linger, so the table never equals the current-state oracle.
"""


def iceberg_target_ddl(table: str) -> str:
    # BUG: no PRIMARY KEY -> append-only, not upsert-capable.
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT) WITH ('format-version' = '2')"


def paimon_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ('bucket' = '1')"
    )


def apply_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    return (
        f"CREATE TEMPORARY TABLE {name} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ("
        f"'connector' = 'filesystem', 'path' = '{path}', "
        f"'format' = 'debezium-json')"
    )

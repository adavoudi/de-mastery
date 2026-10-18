"""MUTANT: the Paimon target has NO primary key (an append-only table), so the
LSM never merges by key — every change event is appended. The Paimon table then
disagrees with the current-state oracle (and with the Iceberg table).
"""


def iceberg_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')"
    )


def paimon_target_ddl(table: str) -> str:
    # BUG: no PRIMARY KEY -> append-only Paimon table, no merge by key.
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT) WITH ('bucket' = '1')"


def apply_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    return (
        f"CREATE TEMPORARY TABLE {name} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ("
        f"'connector' = 'filesystem', 'path' = '{path}', "
        f"'format' = 'debezium-json')"
    )

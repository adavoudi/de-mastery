"""Alt A — same behaviour, written differently: table properties in a different
order, an explicit column list in the INSERT, and the Paimon table with an
explicit 'merge-engine'='deduplicate' (the default) spelled out. All four
functions still produce an upsert-capable Iceberg table, a PK Paimon table and a
bounded TEMPORARY debezium source, so the result reconciles identically.
"""


def iceberg_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('write.upsert.enabled' = 'true', 'format-version' = '2')"
    )


def paimon_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('bucket' = '1', 'merge-engine' = 'deduplicate')"
    )


def apply_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} (id, name, amt) SELECT id, name, amt FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    return (
        f"CREATE TEMPORARY TABLE {name} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('format' = 'debezium-json', 'connector' = 'filesystem', "
        f"'path' = '{path}')"
    )

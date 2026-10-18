"""Alt B — a genuinely different storage configuration that yields the same
final state. The Iceberg v2 table relies on the PRIMARY KEY alone (no explicit
'write.upsert.enabled'); Flink still routes +U/-D by key for a v2 PK table. The
Paimon table additionally declares 'changelog-producer'='input'. Both still
reconcile and still commit one snapshot per job.
"""


def iceberg_target_ddl(table: str) -> str:
    # PK + v2 alone is enough for key-routed updates/deletes.
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ('format-version' = '2')"
    )


def paimon_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('bucket' = '1', 'changelog-producer' = 'input')"
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

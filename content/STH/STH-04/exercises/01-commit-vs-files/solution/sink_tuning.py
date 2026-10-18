"""Reference: SQL for streaming a Debezium CDC feed into Iceberg and Paimon.

The test supplies the Iceberg/Paimon catalogs and drives the jobs; these
functions return the DDL + the apply statement. The Iceberg target is
upsert-capable (PK + format-version 2 + write.upsert.enabled) so -D/+U events
apply by key; the Paimon target is a fixed-bucket primary-key table whose LSM
merges by key. The CDC source is a BOUNDED, TEMPORARY debezium-json filesystem
table so the streaming sink finishes and commits (one snapshot per job).
"""


def iceberg_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')"
    )


def paimon_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('bucket' = '1')"
    )


def apply_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    return (
        f"CREATE TEMPORARY TABLE {name} ("
        f"  id INT, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('connector' = 'filesystem', 'path' = '{path}',"
        f"        'format' = 'debezium-json')"
    )

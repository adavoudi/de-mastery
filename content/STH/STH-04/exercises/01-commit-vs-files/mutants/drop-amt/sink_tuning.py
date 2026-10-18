"""MUTANT: the apply statement drops the `amt` column (SELECT id, name) and lets
it default to NULL in the target. The final rows then have NULL amounts and do
not reconcile with the oracle (which carries the real amounts).
"""


def iceberg_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) "
        f"WITH ('format-version' = '2', 'write.upsert.enabled' = 'true')"
    )


def paimon_target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ('bucket' = '1')"
    )


def apply_sql(src: str, tgt: str) -> str:
    # BUG: drops amt, inserting NULL for the amount.
    return f"INSERT INTO {tgt} (id, name) SELECT id, name FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    return (
        f"CREATE TEMPORARY TABLE {name} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ("
        f"'connector' = 'filesystem', 'path' = '{path}', "
        f"'format' = 'debezium-json')"
    )

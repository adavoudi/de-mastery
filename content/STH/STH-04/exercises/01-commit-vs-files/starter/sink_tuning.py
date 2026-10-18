"""Starter — make the CDC mirror reflect the CURRENT state and keep the source
BOUNDED + TEMPORARY. The stubs below create an append-only Iceberg table (no PK,
no upsert) and persist the source in the catalog, so deletes/updates never apply
and the table will not reconcile with the oracle (G1 fails) until you fix them.
See README.
"""


def iceberg_target_ddl(table: str) -> str:
    # TODO: upsert-capable — add PRIMARY KEY (id) NOT ENFORCED and
    #       WITH ('format-version'='2','write.upsert.enabled'='true').
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT)"


def paimon_target_ddl(table: str) -> str:
    # TODO: primary-key table WITH ('bucket'='1').
    return f"CREATE TABLE {table} (id INT, name STRING, amt INT) WITH ('bucket' = '1')"


def apply_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT id, name, amt FROM {src}"


def cdc_source_ddl(name: str, path: str) -> str:
    # TODO: make this a CREATE TEMPORARY TABLE (a filesystem/debezium-json table
    #       cannot be persisted in the Iceberg/Paimon catalog).
    return (
        f"CREATE TABLE {name} (id INT, name STRING, amt INT, "
        f"PRIMARY KEY (id) NOT ENFORCED) WITH ('connector'='filesystem', "
        f"'path'='{path}', 'format'='debezium-json')"
    )

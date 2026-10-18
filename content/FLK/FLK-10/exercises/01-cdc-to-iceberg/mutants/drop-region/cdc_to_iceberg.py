"""MUTANT: the phase-2 upsert forgets to carry the new `region` column, so after
schema evolution the new/updated rows have NULL region and the table does not
reconcile with the source."""


def target_ddl(table: str) -> str:
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
    # BUG: forgot to carry region; new rows get NULL region, so id=3 and the
    # updated id=1 do not reconcile.
    return f"INSERT INTO {tgt} (id, name, amt) SELECT id, name, amt FROM {src}"

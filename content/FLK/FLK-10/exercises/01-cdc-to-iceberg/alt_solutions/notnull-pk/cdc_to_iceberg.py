"""Alt B — NOT NULL primary key column, and the upsert written as a projection
over an aliased subquery (same result)."""


def target_ddl(table: str) -> str:
    return (
        f"CREATE TABLE {table} ("
        f"  id INT NOT NULL, name STRING, amt INT,"
        f"  PRIMARY KEY (id) NOT ENFORCED"
        f") WITH ('format-version' = '2')"
    )


def upsert_sql(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT t.id, t.name, t.amt FROM {src} AS t"


def evolve_ddl(table: str) -> str:
    return f"ALTER TABLE {table} ADD (region STRING)"


def upsert_sql_v2(src: str, tgt: str) -> str:
    return f"INSERT INTO {tgt} SELECT t.id, t.name, t.amt, t.region FROM {src} AS t"

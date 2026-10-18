the Iceberg table's primary key is on name instead of id, so updates/deletes keyed by id are applied against the wrong key and the table misreconciles.

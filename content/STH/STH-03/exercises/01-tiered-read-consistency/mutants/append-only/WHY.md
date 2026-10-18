Creates an append-only table with no primary key, so every write is retained; a scan returns multiple rows per id instead of the collapsed latest value, breaking read consistency.

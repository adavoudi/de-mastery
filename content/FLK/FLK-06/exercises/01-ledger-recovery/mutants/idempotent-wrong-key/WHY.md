Idempotent sink keyed by id%4 instead of the id, so all records in a key-group overwrite one file — only 4 survive, the rest are lost.

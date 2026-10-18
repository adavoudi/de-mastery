target table has no primary key, so Flink cannot route updates/deletes by key; it appends every change and the table never reflects the current (deleted/updated) source state.

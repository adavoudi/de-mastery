fix_under_replicated never restarts the dead broker, so the under-replicated partitions are never repaired and health is not restored.

compact() carries the old delete files forward instead of clearing them, so deletes get double-applied by position against the rewritten file.

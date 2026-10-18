OffsetFetch returns the log-end offset instead of the committed offset, so it never reads -1 for an uncommitted group.

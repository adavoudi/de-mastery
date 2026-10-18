It folds events in arrival (shuffled) order instead of sorting per key by (ts_ms, lsn), so versions open/close out of commit order and windows are wrong.

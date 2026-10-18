Uses a plain non-transactional producer, so copy_then_abort cannot roll back — its "aborted" records are actually written and visible to read_committed.

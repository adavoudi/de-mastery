copy_then_abort commits the transaction instead of aborting, so the records meant to be discarded are visible to a read_committed consumer.

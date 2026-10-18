Uses Python's salted hash(), so partition_for returns different partitions in a fresh interpreter — the cross-process determinism property fails.

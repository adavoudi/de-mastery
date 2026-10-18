added rebalance() but left the heavy map at parallelism 1, so a single subtask still does all the work — the bottleneck is not removed.

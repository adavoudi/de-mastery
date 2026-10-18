over_running_sum uses GROUP BY (collapsing rows into one per key) instead of an OVER window, so it emits an updating aggregate, not one appended running-sum row per input.

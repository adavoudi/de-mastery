the function reads ValueState but never calls cnt.update(n), so the state is never persisted; every record sees an empty state and writes 1, so the count never accumulates (final 1, not 10).

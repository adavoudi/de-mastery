Tracks the last event time instead of the running max, so a late small-timestamp event drags the watermark backwards — violates monotonicity.

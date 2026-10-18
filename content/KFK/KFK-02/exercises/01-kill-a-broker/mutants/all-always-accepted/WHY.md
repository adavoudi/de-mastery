Ignores min.insync.replicas, predicting an acks=all write is accepted even when ISR < min_isr — misses the NOT_ENOUGH_REPLICAS rejection.

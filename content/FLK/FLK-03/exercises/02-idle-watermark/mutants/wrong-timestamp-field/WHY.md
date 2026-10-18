timestamp assigner reads amountCents instead of tsMs, so event time is taken from the wrong field (returns 10, not 777).

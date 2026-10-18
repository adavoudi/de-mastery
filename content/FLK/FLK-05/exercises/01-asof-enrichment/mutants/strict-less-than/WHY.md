Uses a strict effFrom < ts instead of <=, so an order placed exactly when a new version becomes effective misses that version (off-by-one on the effective boundary).

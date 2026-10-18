Assigns replicas from scratch instead of keeping valid current replicas, so it moves far more data than necessary (violates minimal movement and the live-apply reuse check).

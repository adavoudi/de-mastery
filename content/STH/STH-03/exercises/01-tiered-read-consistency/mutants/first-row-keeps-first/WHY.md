Uses the first-row merge engine, which keeps the first write per id and ignores later upserts, so the final balance is the original value instead of the latest.

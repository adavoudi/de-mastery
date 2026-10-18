delete() rewrites the data file in place (copy-on-write) instead of writing a merge-on-read delete file, violating immutability.

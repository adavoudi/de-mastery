Commits the offset before doing the work, so a crash in sink() loses that record (at-most-once).

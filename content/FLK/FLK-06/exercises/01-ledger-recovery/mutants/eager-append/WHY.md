Eager at-least-once append sink: writes on every record with no commit-on-checkpoint, so records replayed after the crash are appended again — duplicates.

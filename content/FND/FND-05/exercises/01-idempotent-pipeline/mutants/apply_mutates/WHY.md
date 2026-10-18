apply_idempotent mutates the caller's state in place and overwrites existing keys (last-write-wins) — a non-idempotent apply that corrupts committed state on replay.

Compares only last_log_index and ignores last_log_term, so a longer-but-staler log looks current — can elect a leader missing committed entries.

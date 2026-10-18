On conflict it retries with the STALE base metadata (re-reads version number but not the manifest), losing the winner's rows.

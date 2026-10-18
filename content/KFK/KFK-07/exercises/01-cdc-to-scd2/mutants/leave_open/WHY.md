On update it opens a new version without closing the old one, leaving multiple is_current=True rows per key with valid_to=None (overlapping windows).

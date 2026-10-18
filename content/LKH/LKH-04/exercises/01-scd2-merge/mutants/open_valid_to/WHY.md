Sets is_current=false but never sets valid_to, leaving the superseded row 'open' so a key has two rows with NULL valid_to.

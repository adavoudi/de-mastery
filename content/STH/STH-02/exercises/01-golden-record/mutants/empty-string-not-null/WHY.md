Writes empty strings instead of SQL NULL for unowned columns; partial-update treats '' as non-null, so a later source overwrites an earlier real value with a blank.

A non-streaming, non-idempotent sink re-appends the entire directory on every call, so repeated runs multiply the rows.

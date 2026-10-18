"""LKH-02 / 01-retry-commit — starter. See README.md."""


def read_balance(table):
    # TODO: return the current balance for id=1 from the given table.
    raise NotImplementedError


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    # TODO: refresh-and-reapply loop. Load the table fresh inside the loop, read the
    # current balance, compute new = mutate(current), call before_commit(attempt) if
    # given (after reading, before writing), overwrite id=1, and retry on
    # CommitFailedException.
    raise NotImplementedError

"""Mutant: no refresh — loads once before the loop and reuses the stale read."""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    table = catalog.load_table(identifier)   # WRONG: loaded ONCE, never refreshed
    current = read_balance(table)            # WRONG: read ONCE, stale after a conflict
    new = mutate(current)
    for attempt in range(max_retries):
        if before_commit is not None:
            before_commit(attempt)
        row = pa.table({"id": [1], "bal": [new]}, schema=table.schema().as_arrow())
        try:
            table.overwrite(row, overwrite_filter=EqualTo("id", 1))
            return new
        except CommitFailedException:
            if attempt == max_retries - 1:
                raise
    raise CommitFailedException("exhausted retries")  # pragma: no cover

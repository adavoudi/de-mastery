"""Mutant: blind retry — recomputes nothing; writes the first value again.

It reloads the table on retry, but keeps the balance it computed from the FIRST
(stale) read and just forces that value in, overwriting the winner. Lost update.
"""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    new = None
    for attempt in range(max_retries):
        table = catalog.load_table(identifier)
        if new is None:                       # WRONG: compute only on the first attempt
            new = mutate(read_balance(table))
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

"""LKH-02 / 01-retry-commit — reference solution.

Refresh-and-reapply: each attempt reloads the table (picking up any winner's
commit), recomputes the change on that fresh base, and retries on conflict.
"""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    for attempt in range(max_retries):
        table = catalog.load_table(identifier)        # fresh load each attempt
        current = read_balance(table)
        new = mutate(current)
        if before_commit is not None:
            before_commit(attempt)                    # test may commit a competitor here
        row = pa.table({"id": [1], "bal": [new]}, schema=table.schema().as_arrow())
        try:
            table.overwrite(row, overwrite_filter=EqualTo("id", 1))
            return new
        except CommitFailedException:
            if attempt == max_retries - 1:
                raise
            # loop: reload and recompute on the winner's state
    raise CommitFailedException("exhausted retries")  # pragma: no cover

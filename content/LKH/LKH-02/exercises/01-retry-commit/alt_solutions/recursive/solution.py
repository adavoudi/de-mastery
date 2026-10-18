"""LKH-02 / 01-retry-commit — alt: recursive refresh-and-reapply.

Same semantics expressed as recursion instead of an explicit loop: on a conflict,
the function calls itself with one more attempt consumed, which reloads the table
and recomputes on the fresh state.
"""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def _attempt(catalog, identifier, mutate, attempt, max_retries, before_commit):
    table = catalog.load_table(identifier)            # fresh load this attempt
    current = read_balance(table)
    new = mutate(current)
    if before_commit is not None:
        before_commit(attempt)
    row = pa.table({"id": [1], "bal": [new]}, schema=table.schema().as_arrow())
    try:
        table.overwrite(row, overwrite_filter=EqualTo("id", 1))
        return new
    except CommitFailedException:
        if attempt + 1 >= max_retries:
            raise
        return _attempt(catalog, identifier, mutate, attempt + 1, max_retries,
                        before_commit)


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    return _attempt(catalog, identifier, mutate, 0, max_retries, before_commit)

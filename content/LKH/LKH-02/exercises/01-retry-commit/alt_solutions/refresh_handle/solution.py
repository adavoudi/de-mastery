"""LKH-02 / 01-retry-commit — alt: keep one table handle and refresh() it.

Instead of reloading from the catalog each attempt, hold one Table object and call
.refresh() to pull in a competing commit before recomputing. Same invariant, a
different refresh mechanism.
"""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    table = catalog.load_table(identifier)
    for attempt in range(max_retries):
        table.refresh()                               # pull in any winner's commit
        current = read_balance(table)
        new = mutate(current)
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

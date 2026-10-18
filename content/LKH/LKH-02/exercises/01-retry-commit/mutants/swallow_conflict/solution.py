"""Mutant: swallows CommitFailedException and returns as if it had committed.

A single attempt; on conflict it catches the exception and returns the value it
tried to write, so the caller believes the write succeeded. Silent lost write.
"""

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo


def read_balance(table):
    arr = table.scan(row_filter=EqualTo("id", 1)).to_arrow()
    return arr.column("bal").to_pylist()[0]


def commit_with_retry(catalog, identifier, mutate, max_retries=5, before_commit=None):
    table = catalog.load_table(identifier)
    current = read_balance(table)
    new = mutate(current)
    if before_commit is not None:
        before_commit(0)
    row = pa.table({"id": [1], "bal": [new]}, schema=table.schema().as_arrow())
    try:
        table.overwrite(row, overwrite_filter=EqualTo("id", 1))
    except CommitFailedException:
        pass                                  # WRONG: swallow -> silent lost write
    return new

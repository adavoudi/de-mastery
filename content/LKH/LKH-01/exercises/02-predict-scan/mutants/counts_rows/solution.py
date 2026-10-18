"""Mutant: reads data and counts matching ROWS, not the FILES scanned."""

from pyiceberg.expressions import EqualTo, GreaterThanOrEqual


def _rows(table, expr):
    # WRONG: materializes rows and reports a row count / row-derived "paths".
    tbl = table.scan(row_filter=expr).to_arrow()
    n = tbl.num_rows
    return {"num_files": n, "paths": [f"row-{i}" for i in range(n)]}


def files_for_equals(table, column, value):
    return _rows(table, EqualTo(column, value))


def files_for_at_least(table, column, value):
    return _rows(table, GreaterThanOrEqual(column, value))


def all_files(table):
    return sorted(task.file.file_path for task in table.scan().plan_files())

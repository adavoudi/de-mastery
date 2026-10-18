"""Mutant: uses strict > for the 'at least' (>=) predicate — off by one.

For a value that lies exactly at a file's boundary, '>' wrongly prunes the file
that '>=' must keep.
"""

from pyiceberg.expressions import EqualTo, GreaterThan


def _plan_paths(table, expr):
    scan = table.scan() if expr is None else table.scan(row_filter=expr)
    return sorted(task.file.file_path for task in scan.plan_files())


def files_for_equals(table, column, value):
    paths = _plan_paths(table, EqualTo(column, value))
    return {"num_files": len(paths), "paths": paths}


def files_for_at_least(table, column, value):
    # WRONG: strict greater-than drops the boundary value.
    paths = _plan_paths(table, GreaterThan(column, value))
    return {"num_files": len(paths), "paths": paths}


def all_files(table):
    return _plan_paths(table, None)

"""LKH-01 / 02-predict-scan — reference solution.

Uses Iceberg scan planning: build a pyiceberg predicate, call plan_files(), and
count the surviving FileScanTasks. Pruning is driven by the manifests' per-file
min/max, so no data file that cannot match is read.
"""

from pyiceberg.expressions import EqualTo, GreaterThanOrEqual


def _plan_paths(table, expr):
    scan = table.scan() if expr is None else table.scan(row_filter=expr)
    return sorted(task.file.file_path for task in scan.plan_files())


def files_for_equals(table, column, value):
    paths = _plan_paths(table, EqualTo(column, value))
    return {"num_files": len(paths), "paths": paths}


def files_for_at_least(table, column, value):
    paths = _plan_paths(table, GreaterThanOrEqual(column, value))
    return {"num_files": len(paths), "paths": paths}


def all_files(table):
    return _plan_paths(table, None)

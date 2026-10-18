"""LKH-01 / 02-predict-scan — alt: string predicates.

PyIceberg also accepts a SQL-ish predicate string as row_filter; it is parsed into
the same expression tree and pruned the same way. Genuinely different surface, same
planning result.
"""


def _plan_paths(table, expr):
    scan = table.scan() if expr is None else table.scan(row_filter=expr)
    return sorted(task.file.file_path for task in scan.plan_files())


def _quote(value):
    return f"'{value}'" if isinstance(value, str) else str(value)


def files_for_equals(table, column, value):
    paths = _plan_paths(table, f"{column} == {_quote(value)}")
    return {"num_files": len(paths), "paths": paths}


def files_for_at_least(table, column, value):
    paths = _plan_paths(table, f"{column} >= {_quote(value)}")
    return {"num_files": len(paths), "paths": paths}


def all_files(table):
    return _plan_paths(table, None)

"""Mutant: ignores pruning — returns ALL files regardless of the filter."""


def _all(table):
    return sorted(task.file.file_path for task in table.scan().plan_files())


def files_for_equals(table, column, value):
    paths = _all(table)  # WRONG: never prunes.
    return {"num_files": len(paths), "paths": paths}


def files_for_at_least(table, column, value):
    paths = _all(table)  # WRONG: never prunes.
    return {"num_files": len(paths), "paths": paths}


def all_files(table):
    return _all(table)

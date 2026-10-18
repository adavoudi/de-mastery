"""LKH-01 / 02-predict-scan — alt: manual pruning from manifest min/max.

Reads each data file's per-column lower/upper bounds straight from the manifest
entries and keeps only files whose [min, max] range can satisfy the predicate.
This is exactly what the planner does, done by hand, and must agree with it.
"""

from pyiceberg.conversions import from_bytes


def _field(table, column):
    f = table.schema().find_field(column)
    return f.field_id, f.field_type


def _bounds(table, column):
    """Yield (file_path, lower, upper) decoded to Python values for `column`."""
    fid, ftype = _field(table, column)
    snap = table.current_snapshot()
    if snap is None:
        return
    for manifest in snap.manifests(table.io):
        for entry in manifest.fetch_manifest_entry(table.io):
            df = entry.data_file
            lo = from_bytes(ftype, df.lower_bounds[fid])
            hi = from_bytes(ftype, df.upper_bounds[fid])
            yield df.file_path, lo, hi


def files_for_equals(table, column, value):
    keep = sorted(
        p for p, lo, hi in _bounds(table, column) if lo <= value <= hi
    )
    return {"num_files": len(keep), "paths": keep}


def files_for_at_least(table, column, value):
    keep = sorted(p for p, lo, hi in _bounds(table, column) if hi >= value)
    return {"num_files": len(keep), "paths": keep}


def all_files(table):
    return sorted(task.file.file_path for task in table.scan().plan_files())

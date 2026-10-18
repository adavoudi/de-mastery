"""LKH-01 / 01-walk-metadata — alt: enumerate data files via scan planning.

A full (unfiltered) scan plans one FileScanTask per live data file; each task's
.file carries file_path and record_count. The manifest/snapshot counts still come
from the snapshot's manifest list.
"""


def _tasks(table):
    return list(table.scan().plan_files())


def walk(table):
    tasks = _tasks(table)
    snap = table.current_snapshot()
    manifests = snap.manifests(table.io)
    paths = [t.file.file_path for t in tasks]
    total = sum(t.file.record_count for t in tasks)
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": len(list(table.snapshots())),
        "num_manifests": len(manifests),
        "num_data_files": len(paths),
        "total_records": total,
        "data_file_paths": paths,
    }


def records_per_file(table):
    return {t.file.file_path: t.file.record_count for t in _tasks(table)}

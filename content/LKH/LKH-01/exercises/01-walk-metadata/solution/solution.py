"""LKH-01 / 01-walk-metadata — reference solution.

Walks the current snapshot: manifest list -> manifests -> manifest entries ->
data files, reading per-file record_count from the manifest (no data scan).
"""


def _current_data_entries(table):
    """Yield (file_path, record_count) for every data file in the current snapshot."""
    snap = table.current_snapshot()
    if snap is None:
        return
    for manifest in snap.manifests(table.io):
        for entry in manifest.fetch_manifest_entry(table.io):
            df = entry.data_file
            yield df.file_path, df.record_count


def walk(table):
    snap = table.current_snapshot()
    manifests = snap.manifests(table.io)
    paths = []
    total = 0
    for path, rc in _current_data_entries(table):
        paths.append(path)
        total += rc
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": len(list(table.snapshots())),
        "num_manifests": len(manifests),
        "num_data_files": len(paths),
        "total_records": total,
        "data_file_paths": paths,
    }


def records_per_file(table):
    return {path: rc for path, rc in _current_data_entries(table)}

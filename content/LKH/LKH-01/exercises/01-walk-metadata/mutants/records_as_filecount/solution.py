"""Mutant: total_records read from the wrong level (counts files, not rows)."""


def _entries(table):
    snap = table.current_snapshot()
    for manifest in snap.manifests(table.io):
        for entry in manifest.fetch_manifest_entry(table.io):
            yield entry.data_file.file_path, entry.data_file.record_count


def walk(table):
    snap = table.current_snapshot()
    manifests = snap.manifests(table.io)
    paths = [p for p, _ in _entries(table)]
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": len(list(table.snapshots())),
        "num_manifests": len(manifests),
        "num_data_files": len(paths),
        # WRONG: uses the file count as the record count (ignores record_count stat).
        "total_records": len(paths),
        "data_file_paths": paths,
    }


def records_per_file(table):
    # WRONG: pretends every file holds exactly one record.
    return {p: 1 for p, _ in _entries(table)}

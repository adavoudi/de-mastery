"""Mutant: walks ALL snapshots' manifests, double-counting superseded files."""


def _entries(table):
    # WRONG: iterates every snapshot, not just the current one, so files that
    # appear in multiple snapshots' manifest lists are counted repeatedly.
    for snap in table.snapshots():
        for manifest in snap.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                yield entry.data_file.file_path, entry.data_file.record_count


def walk(table):
    cur = table.current_snapshot()
    manifests = cur.manifests(table.io)
    paths = [p for p, _ in _entries(table)]
    total = sum(rc for _, rc in _entries(table))
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": len(list(table.snapshots())),
        "num_manifests": len(manifests),
        "num_data_files": len(paths),
        "total_records": total,
        "data_file_paths": paths,
    }


def records_per_file(table):
    return {p: rc for p, rc in _entries(table)}

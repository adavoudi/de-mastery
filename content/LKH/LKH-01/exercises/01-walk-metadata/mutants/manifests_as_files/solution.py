"""Mutant: returns the MANIFEST paths as the data-file list (conflates levels).

A manifest is an index of files, not a data file. Treating manifest paths as the
data files (and their count as the data-file count) confuses two levels of the tree.
"""


def walk(table):
    snap = table.current_snapshot()
    manifests = snap.manifests(table.io)
    # WRONG: manifest .avro paths, not the .parquet data-file paths.
    manifest_paths = [m.manifest_path for m in manifests]
    total = 0
    for m in manifests:
        for entry in m.fetch_manifest_entry(table.io):
            total += entry.data_file.record_count
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": len(list(table.snapshots())),
        "num_manifests": len(manifests),
        "num_data_files": len(manifest_paths),
        "total_records": total,
        "data_file_paths": manifest_paths,
    }


def records_per_file(table):
    # WRONG: keyed by manifest path, not data-file path.
    snap = table.current_snapshot()
    out = {}
    for m in snap.manifests(table.io):
        rc = sum(e.data_file.record_count for e in m.fetch_manifest_entry(table.io))
        out[m.manifest_path] = rc
    return out

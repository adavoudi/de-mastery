"""LKH-01 / 01-walk-metadata — alt: use table.inspect metadata tables.

Instead of fetching manifest entries by hand, read the same facts from the
inspection tables (files / manifests / snapshots), which are Arrow tables.
"""


def walk(table):
    files = table.inspect.files()  # current-snapshot files (content, file_path, record_count...)
    paths = files.column("file_path").to_pylist()
    counts = files.column("record_count").to_pylist()
    # content 0 == data file; this table has only data files, but be explicit.
    contents = files.column("content").to_pylist()
    data = [(p, c) for p, c, k in zip(paths, counts, contents) if k == 0]
    manifests = table.inspect.manifests()
    return {
        "metadata_location": table.metadata_location,
        "num_snapshots": table.inspect.snapshots().num_rows,
        "num_manifests": manifests.num_rows,
        "num_data_files": len(data),
        "total_records": sum(c for _, c in data),
        "data_file_paths": [p for p, _ in data],
    }


def records_per_file(table):
    files = table.inspect.files()
    out = {}
    for p, c, k in zip(
        files.column("file_path").to_pylist(),
        files.column("record_count").to_pylist(),
        files.column("content").to_pylist(),
    ):
        if k == 0:
            out[p] = c
    return out

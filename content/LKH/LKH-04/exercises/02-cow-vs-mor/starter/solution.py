"""LKH-04 / 02-cow-vs-mor — starter. See README.md."""


def create_table(spark, ident, mode):
    # TODO: CREATE a format-version 2 Iceberg table (id INT, cc STRING) with the
    # delete/update/merge modes set to `mode` ('copy-on-write' or 'merge-on-read').
    raise NotImplementedError


def files_by_content(spark, ident):
    # TODO: return {content: count} from the {ident}.files metadata table.
    raise NotImplementedError


def data_file_paths(spark, ident):
    # TODO: return the set of content=0 (data) file_path values from {ident}.files.
    raise NotImplementedError

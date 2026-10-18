# Prove copy-on-write vs merge-on-read from metadata

Do not guess which row-level strategy a table uses. Read it from the metadata. You create two
tables, one copy-on-write and one merge-on-read. You delete a row from each table. Then you show
the measurable difference. Implement this in `solution.py`:

## `create_table(spark, ident, mode)`

Create a **format-version 2** Iceberg table `ident` with the columns `(id INT, cc STRING)`. Set the
delete, update and merge modes of the table all to `mode`. The value of `mode` is either
`"copy-on-write"` or `"merge-on-read"`. (In `TBLPROPERTIES`, set `write.delete.mode`,
`write.update.mode`, `write.merge.mode` and `format-version=2`.)

## `files_by_content(spark, ident) -> dict[int, int]`

Read the `ident.files` metadata table. Return a mapping `content -> count`. The value
`content = 0` is a data file, `1` is a position-delete file, and `2` is an equality-delete file.

## `data_file_paths(spark, ident) -> set[str]`

Return the set of `file_path` values for the current **data** files (content = 0) from
`ident.files`.

Rules, and what the metadata should show after a `DELETE`:
- **Merge-on-read:** a position-delete file appears. `files_by_content` includes `content = 1`. The
  set of content-0 data files is **unchanged** (no rewrite).
- **Copy-on-write:** **no** delete file appears (no `content = 1`). Iceberg **rewrites** the
  affected data file. So the set `data_file_paths` of content-0 files **changes** (a new file
  replaces the old file).

The test creates both tables with the same data and deletes the same row. It asserts these
measurable properties from the metadata. It never asserts timing.

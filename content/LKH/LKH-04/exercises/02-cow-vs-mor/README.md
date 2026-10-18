# Prove copy-on-write vs merge-on-read from metadata

Don't guess which row-level strategy a table uses — read it from the metadata. You will create two
tables, one copy-on-write and one merge-on-read, delete a row from each, and expose the measurable
difference. Implement in `solution.py`:

## `create_table(spark, ident, mode)`

Create a **format-version 2** Iceberg table `ident` with columns `(id INT, cc STRING)` whose
delete/update/merge modes are all set to `mode`, where `mode` is either `"copy-on-write"` or
`"merge-on-read"`. (Set `write.delete.mode`, `write.update.mode`, `write.merge.mode` plus
`format-version=2` in `TBLPROPERTIES`.)

## `files_by_content(spark, ident) -> dict[int, int]`

Read the `ident.files` metadata table and return a mapping `content -> count`:
`content = 0` are data files, `1` are position-delete files, `2` are equality-delete files.

## `data_file_paths(spark, ident) -> set[str]`

Return the set of `file_path` values for the current **data** files (content = 0) from
`ident.files`.

Rules / what the metadata should show after a `DELETE`:
- **Merge-on-read:** a position-delete file appears → `files_by_content` includes `content = 1`,
  and the content-0 data-file set is **unchanged** (no rewrite).
- **Copy-on-write:** **no** delete file (no `content = 1`) → the affected data file is **rewritten**,
  so the content-0 `data_file_paths` set **changes** (a new file replaces the old).

The test creates both tables with the same data, deletes the same row, and asserts these
measurable properties from the metadata — never timing.

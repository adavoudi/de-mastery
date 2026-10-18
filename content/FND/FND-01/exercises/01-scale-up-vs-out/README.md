# One big box vs. a process pool — and why bytes-read decides

You will compute the **same aggregation two ways** over the commerce `order_items` data, and
report how many **bytes each approach had to read**. The lesson's claim is that a single-process
columnar scan can beat a parallel row-oriented scan *because it reads far fewer bytes* — a robust,
machine-independent quantity (unlike wall-clock time).

The aggregation (the "query"): **total `line_amount` and row count per `product_code`**.

Implement these three functions in `solution.py`:

### 1. `agg_duckdb(parquet_path) -> list[dict]`
Use DuckDB to read the **Parquet** file and return rows
`{"product_code": str, "total_amount": float, "n": int}` — the sum of `line_amount` and the count
of rows per `product_code`. Only project the columns you need so the engine prunes the rest.

### 2. `agg_pool(csv_path, n_workers) -> list[dict]`
Compute the **identical** result from the **CSV** file using `multiprocessing.Pool` with
`n_workers` workers. Partition the work (e.g. split the data rows into `n_workers` contiguous
chunks), aggregate each chunk in a worker, then combine the partial `(sum, count)` results. The
combine step must be associative so the final answer does not depend on how the chunks were split
or in what order partials come back.

### 3. `bytes_read(parquet_path, csv_path) -> dict`
Return `{"parquet": int, "csv": int}` where:
- `parquet` is the total **compressed size of only the two column chunks** you actually scan in
  `agg_duckdb` (`product_code` and `line_amount`), summed across all row groups. Read this from
  the Parquet metadata (`pyarrow.parquet.ParquetFile(...).metadata`).
- `csv` is the full **on-disk size of the CSV file** in bytes (the pool scan is row-oriented and
  must read every column of every row).

## What the tests check

- `agg_duckdb` and `agg_pool` both match an **independent DuckDB oracle** (over Parquet and CSV
  respectively), to a float tolerance.
- The two implementations agree with **each other** (two engines, same answer).
- `bytes_read["parquet"] < bytes_read["csv"]` — the columnar+projection path reads strictly fewer
  bytes. (This is the measured quantity the lesson is about; **no timing is asserted**.)
- Your `explain.txt` contains the required reasoning keywords (see below).

## Also write `explain.txt`

A short written explanation (a few sentences). It **must mention**, in your own words, all of:
`amdahl`, `shuffle`, `bytes`, `columnar`, and `serial`. The test checks these keywords appear
(case-insensitive). This forces you to connect the measurement to the concepts.

Constraints: pure Python + `duckdb` + `pyarrow` + `multiprocessing`. Deterministic. `lab: none`.

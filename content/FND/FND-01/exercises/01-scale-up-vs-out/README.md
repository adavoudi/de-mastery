# One big box vs. a process pool — and why bytes-read decides

You will compute the **same aggregation in two ways** over the commerce `order_items` data. You
will also report how many **bytes each approach had to read**. The lesson claims that a
single-process columnar scan can beat a parallel row-oriented scan. The reason is that it reads
far fewer bytes. The number of bytes does not depend on the machine. Wall-clock time does.

The aggregation (the "query") is: **total `line_amount` and row count per `product_code`**.

Implement these three functions in `solution.py`:

### 1. `agg_duckdb(parquet_path) -> list[dict]`
Use DuckDB to read the **Parquet** file. Return rows
`{"product_code": str, "total_amount": float, "n": int}`. These rows hold the sum of
`line_amount` and the count of rows per `product_code`. Project only the columns that you need,
so that the engine prunes the rest.

### 2. `agg_pool(csv_path, n_workers) -> list[dict]`
Compute the **identical** result from the **CSV** file. Use `multiprocessing.Pool` with
`n_workers` workers. Follow these steps:

1. Partition the work (for example, split the data rows into `n_workers` contiguous chunks).
2. Aggregate each chunk in one worker.
3. Combine the partial `(sum, count)` results.

The combine step must be associative. Then the final answer does not depend on how you split the
chunks, or on the order in which the partial results come back.

### 3. `bytes_read(parquet_path, csv_path) -> dict`
Return `{"parquet": int, "csv": int}` where:
- `parquet` is the total **compressed size of only the two column chunks** that you actually scan
  in `agg_duckdb` (`product_code` and `line_amount`), summed across all row groups. Read this
  from the Parquet metadata (`pyarrow.parquet.ParquetFile(...).metadata`).
- `csv` is the full **on-disk size of the CSV file** in bytes. The pool scan is row-oriented. It
  must read every column of every row.

## What the tests check

- `agg_duckdb` and `agg_pool` both match an **independent DuckDB oracle** (over Parquet and CSV
  respectively), to a float tolerance.
- The two implementations agree with **each other** (two engines, same answer).
- `bytes_read["parquet"] < bytes_read["csv"]`. The columnar path with projection reads strictly
  fewer bytes. (The lesson is about this measured quantity. **The tests do not assert timing**.)
- Your `explain.txt` contains the required reasoning keywords (see below).

## Also write `explain.txt`

Write a short explanation (a few sentences). It **must mention**, in your own words, all of these
words: `amdahl`, `shuffle`, `bytes`, `columnar`, and `serial`. The test checks that these
keywords appear (case-insensitive). This makes you connect the measurement to the concepts.

Constraints: pure Python + `duckdb` + `pyarrow` + `multiprocessing`. Deterministic. `lab: none`.

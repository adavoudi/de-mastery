"""C3 deterministic data + CDC-ingest hook.

Reuses ``de_data.commerce.generate`` (the same seeded multi-company commerce
dataset C1/C2 use) and derives a **debezium-json changelog** that stands in for
C2's CDC topics so the Flink jobs can read it on a bare machine (NO Docker, no
Kafka). This is the proven STH-04 no-Docker path.

Deterministic for a given ``(seed, scale)``: same inputs -> byte-identical files.
Nothing here iterates an unbounded stream; the changelog is a bounded file.

CLI::

    uv run python capstones/C3/data/generate.py --out /tmp/c3 --seed 42 --scale 0.001

Importable::

    from capstones.C3.data.generate import build_c3_dataset
    paths = build_c3_dataset(out_dir, seed=42, scale=0.001)
    # paths["commerce"] -> dict[str, Path] (parquet); paths["cdc_customers"] etc.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _debezium_lines(rows: list[dict], key: str) -> list[str]:
    """Turn a list of current-state dicts into a bounded debezium-json changelog.

    Each row becomes a create (``op='c'``) event. Updates/deletes are left to the
    learner to extend (M1 SCD2); the scaffolding only needs a bounded, replayable
    file so the Flink source finishes and the sink commits.
    """
    lines: list[str] = []
    for r in rows:
        lines.append(json.dumps({"before": None, "after": r, "op": "c"},
                                 sort_keys=True))
    return lines


def build_c3_dataset(out_dir, *, seed: int = 42, scale: float = 0.001) -> dict:
    """Generate the commerce dataset and derive the C3 CDC changelog files.

    Returns a dict with:
      * ``commerce`` -> dict[name, Path] (the raw parquet tables)
      * ``cdc_customers`` / ``cdc_orders`` / ``cdc_fx`` -> Path to a .jsonl file
      * ``dir`` -> the output Path
    """
    import pyarrow.parquet as pq
    from de_data.commerce import generate

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    commerce = generate(seed=seed, scale=scale, out_dir=out / "commerce")

    def _rows(name: str) -> list[dict]:
        tbl = pq.read_table(commerce[name])
        return tbl.to_pylist()

    cdc = {}
    for name, key in (("customers", "customer_id"),
                      ("orders", "order_id"),
                      ("fx_rates", "rate_date")):
        path = out / f"cdc_{name}.jsonl"
        lines = _debezium_lines(_rows(name), key)
        # Deterministic: sorted keys above + stable row order from the generator.
        path.write_text("\n".join(lines) + ("\n" if lines else ""))
        cdc[f"cdc_{name}"] = path

    return {"dir": out, "commerce": commerce, **cdc}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="C3 deterministic data hook")
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", type=float, default=0.001)
    args = ap.parse_args(argv)
    paths = build_c3_dataset(args.out, seed=args.seed, scale=args.scale)
    n = {k: (len(Path(v).read_text().splitlines()) if str(k).startswith("cdc_")
             else len(v)) for k, v in paths.items() if k != "dir"}
    print(f"C3 dataset written to {paths['dir']}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""FLK-08 — the same per-currency GMV pipeline in Java Table API and PyFlink.

Both surfaces must reproduce one independent Python oracle (SUM(amt) GROUP BY
ccy). The Java Table API side is graded via the embedded `flink_java` runner and
is ALWAYS the graded surface (SKIPs only if the Flink dist/JDK is absent). The
PyFlink side is graded via `pyflink_runner` and SKIPs cleanly if the dedicated
PyFlink venv is not installed — so a machine without PyFlink still grades Java.
"""

import json

# order: {ccy, amtMinor}. Chosen so multiple currencies and multiple rows per
# currency exercise the GROUP BY SUM.
ORDERS = [
    ["EUR", "100"],
    ["USD", "50"],
    ["EUR", "25"],
    ["GBP", "80"],
    ["USD", "200"],
    ["EUR", "75"],
]


def _oracle():
    out = {}
    for ccy, amt in ORDERS:
        out[ccy] = out.get(ccy, 0) + int(amt)
    return out


def _orders_env():
    return {"ORDERS": ";".join(",".join(r) for r in ORDERS)}


_DRIVER = r'''
import java.util.*;
import de.flk.GmvTable;
public class DeDriver {
  static List<String[]> rows(String spec) {
    List<String[]> out = new ArrayList<>();
    if (spec.isEmpty()) return out;
    for (String rec : spec.split(";")) out.add(rec.split(","));
    return out;
  }
  public static void main(String[] a) throws Exception {
    Map<String,Long> res = GmvTable.run(rows(System.getenv("ORDERS")));
    TreeMap<String,Long> s = new TreeMap<>(res);
    StringBuilder sb = new StringBuilder("{");
    boolean f = true;
    for (Map.Entry<String,Long> e : s.entrySet()) {
      if (!f) sb.append(",");
      f = false;
      sb.append("\"").append(e.getKey()).append("\":").append(e.getValue());
    }
    sb.append("}");
    System.out.println("DE_RESULT=" + sb);
  }
}
'''


# ---------------------------------------------------------------------------
# Java Table API side — always the graded surface.
# ---------------------------------------------------------------------------
def test_java_table_api_matches_oracle(flink_java, submission_dir):
    r = flink_java(submission_dir, _DRIVER, env_vars=_orders_env())
    assert r.ok, f"java driver failed rc={r.returncode}\n{r.stderr[-2500:]}"
    got = {k: int(v) for k, v in (r.result or {}).items()}
    assert got == _oracle(), f"\n got={got}\nwant={_oracle()}"


# ---------------------------------------------------------------------------
# PyFlink side — SKIPs cleanly if the venv is absent (extra graded surface).
# ---------------------------------------------------------------------------
def test_pyflink_matches_oracle(pyflink_runner, submission_dir):
    from pathlib import Path
    script = Path(submission_dir) / "job.py"
    # PyFlink boots a full JVM per invocation; under a shuffled multi-run suite a
    # transient process/startup hiccup can occur. Retry a FAILED PROCESS a couple
    # of times (bounded, no sleep-based timing) — but NEVER retry a wrong result,
    # so a genuinely-wrong candidate is still killed.
    r = None
    for _ in range(3):
        r = pyflink_runner(script, env_vars=_orders_env())
        if r.ok:
            break
    assert r.ok, f"pyflink failed rc={r.returncode}\n{r.stderr[-2500:]}"
    got = {k: int(v) for k, v in (r.result or {}).items()}
    assert got == _oracle(), f"\n got={got}\nwant={_oracle()}"
    # Transitively, since the Java test also matches the same oracle, the two
    # surfaces agree — asserted without booting a second JVM in this test.

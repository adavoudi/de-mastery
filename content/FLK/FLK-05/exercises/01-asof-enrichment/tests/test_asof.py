"""FLK-05 — point-in-time (as-of) enrichment of orders with FX rate and address.

The learner builds a Flink DataStream job (temporal/as-of join). We grade on the
enriched result, with an INDEPENDENT Python SCD2 as-of join as the oracle (G6):
for each order, the dimension version chosen is the one with the greatest
effective_from <= the order's event time.

SKIPs cleanly if the vendored Flink dist / JDK is absent (the G1 caveat).
"""

import json

# ---------------------------------------------------------------------------
# Deterministic, bounded inputs chosen so the AS-OF rule actually matters:
#   - EUR has rate 1.00 from t=0, then 1.25 from t=10_000.
#   - USD is constant 1.00 from t=0.
#   - GBP has NO version until t=20_000 (so an early GBP order has no rate).
#   - customer 1 lives in "BER" from t=0, then "MUC" from t=5_000.
#   - customer 2 lives in "LON" from t=0 (never moves).
# Amounts are in minor units; rates are micros (x1e6). usd = amtMinor * rateMicros.
# ---------------------------------------------------------------------------
# order: {orderId, custId, ccy, amtMinor, tsMillis}
ORDERS = [
    ["1", "1", "EUR", "100", "3000"],    # EUR@3000 -> rate 1.00 ; cust1@3000 -> BER
    ["2", "1", "EUR", "200", "12000"],   # EUR@12000 -> rate 1.25 ; cust1@12000 -> MUC
    ["3", "2", "USD", "50", "6000"],     # USD@6000 -> 1.00 ; cust2 -> LON
    ["4", "1", "GBP", "80", "8000"],     # GBP@8000 -> NO rate yet (NULL usd) ; cust1@8000 -> MUC
    ["5", "2", "EUR", "40", "10000"],    # EUR@10000 -> boundary, rate 1.25 (effFrom==ts) ; cust2 -> LON
]
# fx: {ccy, rateMicros, effFromMillis}
FX = [
    ["EUR", "1000000", "0"],
    ["EUR", "1250000", "10000"],
    ["USD", "1000000", "0"],
    ["GBP", "1300000", "20000"],
]
# address: {custId, city, effFromMillis}
ADDR = [
    ["1", "BER", "0"],
    ["1", "MUC", "5000"],
    ["2", "LON", "0"],
]


def _as_of(versions, ts):
    """Greatest effFrom <= ts; None if no version qualifies."""
    best_from = None
    best_val = None
    for val, eff in versions:
        if eff <= ts and (best_from is None or eff >= best_from):
            best_from = eff
            best_val = val
    return best_val


def _oracle():
    fx_by_ccy = {}
    for ccy, rate, eff in FX:
        fx_by_ccy.setdefault(ccy, []).append((int(rate), int(eff)))
    addr_by_cust = {}
    for cust, city, eff in ADDR:
        addr_by_cust.setdefault(cust, []).append((city, int(eff)))
    out = {}
    for oid, cust, ccy, amt, ts in ORDERS:
        ts = int(ts)
        rate = _as_of(fx_by_ccy.get(ccy, []), ts)
        city = _as_of(addr_by_cust.get(cust, []), ts)
        usd = "NULL" if rate is None else str(int(amt) * rate)
        out[int(oid)] = [usd, city if city is not None else "NULL"]
    return out


_DRIVER = r'''
import java.util.*;
import de.flk.AsOfEnrich;
public class DeDriver {
  static List<String[]> rows(String spec) {
    List<String[]> out = new ArrayList<>();
    if (spec.isEmpty()) return out;
    for (String rec : spec.split(";")) out.add(rec.split(","));
    return out;
  }
  public static void main(String[] a) throws Exception {
    List<String[]> orders = rows(System.getenv("ORDERS"));
    List<String[]> fx = rows(System.getenv("FX"));
    List<String[]> addr = rows(System.getenv("ADDR"));
    Map<Integer,String[]> res = AsOfEnrich.run(orders, fx, addr);
    TreeMap<Integer,String[]> s = new TreeMap<>(res);
    StringBuilder sb = new StringBuilder("{");
    boolean f = true;
    for (Map.Entry<Integer,String[]> e : s.entrySet()) {
      if (!f) sb.append(",");
      f = false;
      String[] v = e.getValue();
      sb.append("\"").append(e.getKey()).append("\":[\"")
        .append(v[0]).append("\",\"").append(v[1]).append("\"]");
    }
    sb.append("}");
    System.out.println("DE_RESULT=" + sb);
  }
}
'''


def _env():
    return {
        "ORDERS": ";".join(",".join(r) for r in ORDERS),
        "FX": ";".join(",".join(r) for r in FX),
        "ADDR": ";".join(",".join(r) for r in ADDR),
    }


def _run(flink_java, submission_dir):
    r = flink_java(submission_dir, _DRIVER, env_vars=_env())
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2500:]}"
    # JSON keys come back as strings; normalise to int keys and list values
    return {int(k): list(v) for k, v in (r.result or {}).items()}


def test_asof_enrichment_matches_oracle(flink_java, submission_dir):
    got = _run(flink_java, submission_dir)
    want = _oracle()
    assert got == want, f"\n got={got}\nwant={want}"


def test_rate_change_boundary_is_as_of(flink_java, submission_dir):
    # order 2 is after the EUR rate change (1.25): 200 * 1_250_000.
    # order 1 is before it (1.00): 100 * 1_000_000. This kills "always newest rate".
    got = _run(flink_java, submission_dir)
    assert got[1][0] == str(100 * 1_000_000), got
    assert got[2][0] == str(200 * 1_250_000), got
    # order 5 sits exactly on the boundary ts==effFrom(10000): as-of includes it.
    assert got[5][0] == str(40 * 1_250_000), got


def test_missing_version_is_null(flink_java, submission_dir):
    # GBP has no rate until t=20000; order 4 @8000 must have NULL usd.
    got = _run(flink_java, submission_dir)
    assert got[4][0] == "NULL", got
    # but the address is still resolved as-of (cust1 moved to MUC at 5000).
    assert got[4][1] == "MUC", got


def test_address_as_of(flink_java, submission_dir):
    # cust1 lived in BER before 5000, MUC after. order1@3000 -> BER, order2@12000 -> MUC.
    got = _run(flink_java, submission_dir)
    assert got[1][1] == "BER", got
    assert got[2][1] == "MUC", got

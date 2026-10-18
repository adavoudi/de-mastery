"""FLK-02 — enrich + dead-letter side output (embedded, no Docker).

The learner implements ``de.flk.EnrichOrders.run(orders, companyNames)`` as a
single-pass DataStream job that filters orders to a valid-currency allowlist,
enriches valid orders with the company name, and routes invalid-currency orders
to a side output. The driver calls it and prints
``DE_RESULT={"enriched":[...],"rejects":[...]}``.

Oracle (G6): an INDEPENDENT Python re-implementation of the same filter+enrich+
route decides the expected enriched set and reject set. We compare as SETS so the
(nondeterministic) order across parallel subtasks does not matter.

SKIPs cleanly when the Flink dist / JDK are absent.
"""

# orders: [orderId, companyId, currency, amount]; one invalid currency, one unknown company
ORDERS = [
    ["o1", "c1", "USD", "100"],
    ["o2", "c2", "EUR", "50"],
    ["o3", "c1", "ZZZ", "70"],    # invalid currency -> reject
    ["o4", "c3", "GBP", "20"],    # company c3 not in the dim -> enriched as UNKNOWN
    ["o5", "c2", "usd", "10"],    # lowercase is NOT in the allowlist -> reject
    ["o6", "c1", "JPY", "900"],
]
COMPANY_NAMES = {"c1": "Acme", "c2": "Globex"}
VALID = {"USD", "EUR", "GBP", "JPY"}


def _oracle(orders, names):
    enriched, rejects = [], []
    for order_id, company_id, currency, amount in orders:
        if currency in VALID:
            name = names.get(company_id, "UNKNOWN")
            enriched.append(f"{order_id}|{name}|{currency}|{amount}")
        else:
            rejects.append(f"{order_id}|{company_id}|{currency}")
    return enriched, rejects


_DRIVER = r"""
import de.flk.EnrichOrders;
import java.util.*;

public class DeDriver {
  public static void main(String[] a) throws Exception {
    List<String[]> orders = new ArrayList<>();
%s
    Map<String,String> names = new LinkedHashMap<>();
    names.put("c1","Acme");
    names.put("c2","Globex");
    EnrichOrders.Result r = EnrichOrders.run(orders, names);
    StringBuilder sb = new StringBuilder("{\"enriched\":");
    sb.append(arr(r.enriched));
    sb.append(",\"rejects\":").append(arr(r.rejects)).append("}");
    System.out.println("DE_RESULT=" + sb);
  }

  static String arr(List<String> xs) {
    StringBuilder b = new StringBuilder("[");
    for (int i = 0; i < xs.size(); i++) {
      if (i > 0) b.append(",");
      b.append("\"").append(xs.get(i).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
    }
    return b.append("]").toString();
  }
}
"""


def _driver():
    adds = "\n".join(
        "    orders.add(new String[]{"
        + ",".join(f'"{c}"' for c in row) + "});"
        for row in ORDERS)
    return _DRIVER % adds


def _run(flink_java, submission_dir):
    r = flink_java(submission_dir, _driver())
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2000:]}"
    assert isinstance(r.result, dict), f"bad result {r.result!r}"
    return r.result


def test_enriched_matches_oracle(flink_java, submission_dir):
    out = _run(flink_java, submission_dir)
    exp_enriched, _ = _oracle(ORDERS, COMPANY_NAMES)
    assert set(out["enriched"]) == set(exp_enriched), (
        f"\n got  {sorted(out['enriched'])}\n want {sorted(exp_enriched)}")


def test_rejects_match_oracle(flink_java, submission_dir):
    out = _run(flink_java, submission_dir)
    _, exp_rejects = _oracle(ORDERS, COMPANY_NAMES)
    assert set(out["rejects"]) == set(exp_rejects), (
        f"\n got  {sorted(out['rejects'])}\n want {sorted(exp_rejects)}")


def test_no_record_lost_or_duplicated(flink_java, submission_dir):
    # every input order appears exactly once across the two streams (no loss, no dup)
    out = _run(flink_java, submission_dir)
    ids = [row.split("|")[0] for row in out["enriched"]] + \
          [row.split("|")[0] for row in out["rejects"]]
    assert sorted(ids) == sorted(o[0] for o in ORDERS), ids


def test_unknown_company_enriched_not_dropped(flink_java, submission_dir):
    # o4 has a valid currency but an unknown company -> it must be enriched
    # (as UNKNOWN), NOT dropped or rejected.
    out = _run(flink_java, submission_dir)
    assert any(row.startswith("o4|UNKNOWN|GBP|") for row in out["enriched"]), out
    assert not any(row.startswith("o4|") for row in out["rejects"]), out

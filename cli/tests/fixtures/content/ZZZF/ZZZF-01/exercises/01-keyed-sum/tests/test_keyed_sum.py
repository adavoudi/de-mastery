"""Platform self-test for the `flink_java` fixture (embedded MiniCluster).

Builds a driver that calls the learner's `de.selftest.KeyedSum.run(...)` on an
embedded Flink job and compares against an independent Python oracle (G6).
SKIPs cleanly when the Flink dist / JDK is absent.
"""

import json

# a bounded, deterministic input (key, value-as-string)
ROWS = [("x", 3), ("y", 5), ("x", 4), ("y", 1), ("x", 2)]


def _oracle(rows):
    out = {}
    for k, v in rows:
        out[k] = out.get(k, 0) + v
    return out


_DRIVER = """
import java.util.*;
public class DeDriver {
  public static void main(String[] a) throws Exception {
    List<String[]> rows = new ArrayList<>();
%s
    Map<String,Integer> out = de.selftest.KeyedSum.run(rows);
    TreeMap<String,Integer> s = new TreeMap<>(out);
    StringBuilder sb = new StringBuilder("{");
    boolean f = true;
    for (Map.Entry<String,Integer> e : s.entrySet()) {
      if (!f) sb.append(",");
      f = false;
      sb.append("\\"").append(e.getKey()).append("\\":").append(e.getValue());
    }
    sb.append("}");
    System.out.println("DE_RESULT=" + sb);
  }
}
"""


def _driver():
    adds = "\n".join(
        f'    rows.add(new String[]{{"{k}","{v}"}});' for k, v in ROWS)
    return _DRIVER % adds


def test_keyed_sum(flink_java, submission_dir):
    r = flink_java(submission_dir, _driver())
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2000:]}"
    expected = _oracle(ROWS)
    assert r.result == expected, f"got {r.result} want {expected}"


def test_sum_is_not_min(flink_java, submission_dir):
    # a second assertion that specifically kills the min-not-sum mutant:
    # every key's total must be >= its max single value (sum of positives).
    r = flink_java(submission_dir, _driver())
    assert r.ok, r.stderr[-2000:]
    assert r.result.get("x") == 9 and r.result.get("y") == 6

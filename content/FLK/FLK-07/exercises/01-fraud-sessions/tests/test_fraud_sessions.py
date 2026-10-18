"""FLK-07 — session timeout + fraud rule with broadcast rule updates.

The learner builds a KeyedBroadcastProcessFunction that sessionizes each
account's transactions (gap = GAP) and flags transactions above the fraud
threshold IN FORCE AT the transaction's time (as-of), with the threshold arriving
on a broadcast rule stream. We grade on the per-session (eventCount, flaggedCount)
summary with an INDEPENDENT Python oracle (G6).

SKIPs cleanly if the vendored Flink dist / JDK is absent (G1 caveat).
"""

GAP = 5000

# ---------------------------------------------------------------------------
# Deterministic inputs chosen so BOTH the session boundary AND the as-of rule
# matter:
#   threshold: 150 from t=0, then 300 from t=10_000 (loosened).
#   account "a": two sessions. Session 1 @ {1000:120, 2000:200} (gap to next),
#                session 2 @ {9000:200, 11000:400}.
#     - @1000 amt120 vs thr150 -> not flagged
#     - @2000 amt200 vs thr150 -> FLAGGED
#     - @9000 amt200 vs thr150 (eff until 10000) -> FLAGGED
#     - @11000 amt400 vs thr300 (loosened) -> FLAGGED
#   account "b": one session @ {3000:1000} -> vs thr150 -> FLAGGED.
#   account "c": one session @ {500:50} -> vs thr150 -> not flagged.
# event: {account, amtMinor, tsMillis}
EVENTS = [
    ["a", "120", "1000"],
    ["a", "200", "2000"],
    ["a", "200", "9000"],
    ["a", "400", "11000"],
    ["b", "1000", "3000"],
    ["c", "50", "500"],
]
# rule: {thresholdMinor, effFromMillis}
RULES = [
    ["150", "0"],
    ["300", "10000"],
]


def _as_of(versions, t):
    best_from = None
    best_val = None
    for thr, eff in versions:
        if eff <= t and (best_from is None or eff >= best_from):
            best_from = eff
            best_val = thr
    return best_val


def _oracle():
    versions = [(int(thr), int(eff)) for thr, eff in RULES]
    by_acct = {}
    for acct, amt, ts in EVENTS:
        by_acct.setdefault(acct, []).append((int(ts), int(amt)))
    out = {}
    for acct, evs in by_acct.items():
        evs.sort()
        start = None
        prev = None
        count = 0
        flagged = 0

        def flush():
            if start is not None:
                out[f"{acct}@{start}"] = [count, flagged]

        for ts, amt in evs:
            if start is None or ts - prev >= GAP:
                flush()
                start = ts
                count = 0
                flagged = 0
            count += 1
            thr = _as_of(versions, ts)
            if thr is not None and amt > thr:
                flagged += 1
            prev = ts
        flush()
    return out


_DRIVER = r'''
import java.util.*;
import de.flk.FraudSessions;
public class DeDriver {
  static List<String[]> rows(String spec) {
    List<String[]> out = new ArrayList<>();
    if (spec.isEmpty()) return out;
    for (String rec : spec.split(";")) out.add(rec.split(","));
    return out;
  }
  public static void main(String[] a) throws Exception {
    List<String[]> ev = rows(System.getenv("EVENTS"));
    List<String[]> ru = rows(System.getenv("RULES"));
    Map<String,long[]> res = FraudSessions.run(ev, ru);
    TreeMap<String,long[]> s = new TreeMap<>(res);
    StringBuilder sb = new StringBuilder("{");
    boolean f = true;
    for (Map.Entry<String,long[]> e : s.entrySet()) {
      if (!f) sb.append(",");
      f = false;
      long[] v = e.getValue();
      sb.append("\"").append(e.getKey()).append("\":[")
        .append(v[0]).append(",").append(v[1]).append("]");
    }
    sb.append("}");
    System.out.println("DE_RESULT=" + sb);
  }
}
'''


def _env():
    return {
        "EVENTS": ";".join(",".join(r) for r in EVENTS),
        "RULES": ";".join(",".join(r) for r in RULES),
    }


def _run(flink_java, submission_dir):
    r = flink_java(submission_dir, _DRIVER, env_vars=_env())
    assert r.ok, f"driver failed rc={r.returncode}\n{r.stderr[-2500:]}"
    return {k: list(v) for k, v in (r.result or {}).items()}


def test_sessions_and_flags_match_oracle(flink_java, submission_dir):
    got = _run(flink_java, submission_dir)
    want = _oracle()
    assert got == want, f"\n got={got}\nwant={want}"


def test_two_sessions_for_account_a(flink_java, submission_dir):
    # account a must split into TWO sessions at the >=GAP gap (2000 -> 9000).
    got = _run(flink_java, submission_dir)
    assert "a@1000" in got and "a@9000" in got, got
    assert got["a@1000"][0] == 2, got   # events @1000, @2000
    assert got["a@9000"][0] == 2, got   # events @9000, @11000


def test_as_of_threshold_flags(flink_java, submission_dir):
    # session1: only @2000 (200>150) flagged -> 1 of 2.
    # session2: @9000 (200>150, old rule) and @11000 (400>300, new rule) -> 2 of 2.
    got = _run(flink_java, submission_dir)
    assert got["a@1000"][1] == 1, got
    assert got["a@9000"][1] == 2, got


def test_newest_rule_is_not_used_everywhere(flink_java, submission_dir):
    # If a buggy impl applied the NEWEST threshold (300) to the @9000 event,
    # 200 > 300 is false and the session-2 flag count would drop to 1.
    got = _run(flink_java, submission_dir)
    assert got["a@9000"][1] == 2, (
        "the @9000 event must use the as-of threshold (150), not the newest (300)")

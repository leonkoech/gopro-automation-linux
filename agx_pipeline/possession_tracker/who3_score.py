"""Score the cloud SAM3 WHO run on the unseen games against the live fast setup.

fast  = who_extend.jsonl (production: clip + extended timeline, fast tracks)
sam3  = who3_<game8>.jsonl from who3_cloud.py (clip only, SAM3 tracks)
Policies, each with any number or only the shooting team's roster:
  fast alone | SAM3 alone | fast first, SAM3 only where fast is silent
Also: SAM3 seconds per clip on the cloud GPU.

Usage: who3_score.py <who3 jsonl> [...]   (box, /home/dev/possession)
"""
import json
import os
import sys
from collections import defaultdict

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import who_eval as W  # noqa: E402


def main():
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    roster = defaultdict(set)
    for r in G["rosters"]:
        roster[(r["game"], r["team"])].add(str(r["jersey"]))
    fast = {(j["game"], j["t"]): j for j in map(json.loads, open(os.path.join(HERE, "who_extend.jsonl")))}
    sam = {}
    for p in sys.argv[1:]:
        for j in map(json.loads, open(p)):
            sam[(j["game"], j["t"])] = j
    keys = [k for k in sam if k in fast]
    n = len(keys)
    secs = [sam[k]["sam3_s"] for k in keys if sam[k].get("sam3_s")]
    print("[who3] %d shots scored (games: %s)" % (n, sorted({k[0][:8] for k in keys})))
    if secs:
        print("[who3] SAM3 per clip on the cloud GPU: median %.1f s, p90 %.1f s, max %.1f s"
              % (np.median(secs), np.percentile(secs, 90), max(secs)))
    for label, allowed in (("any number", lambda k: None), ("team roster", lambda k: roster[(k[0], fast[k]["team"])])):
        f = {k: W.speak(fast[k].get("reads") or [], allowed(k)) for k in keys}
        s = {k: W.speak(sam[k].get("reads") or [], allowed(k)) for k in keys}
        for name, ans in (("fast (live)", f), ("SAM3 alone", s),
                          ("fast, SAM3 if silent", {k: f[k] if f[k] is not None else s[k] for k in keys})):
            spoke = sum(v is not None for v in ans.values())
            right = sum(ans[k] == fast[k]["gt"] for k in keys)
            print("[who3] %-11s %-21s spoke %3d (%.0f%%)  right %3d  precision %.1f%%  right of all %.1f%%"
                  % (label, name, spoke, 100 * spoke / n, right, 100 * right / max(1, spoke), 100 * right / n))
        esc = sum(f[k] is None for k in keys)
        fixed = sum(f[k] is None and s[k] == fast[k]["gt"] for k in keys)
        wrong = sum(f[k] is None and s[k] is not None and s[k] != fast[k]["gt"] for k in keys)
        print("[who3] %-11s SAM3 runs on %d/%d silent shots: names %d right, %d wrong" % (label, esc, n, fixed, wrong))


if __name__ == "__main__":
    main()

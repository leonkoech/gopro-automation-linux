"""WHO on SAM3 tracks vs fast tracks, same shots, same jersey reader.

The development games (cb9e1294, 7cef734e) were perceived both ways: SAM3 (out/, out7/) and fast
YOLO-seg + ByteTrack (out_fast/, out7_fast/). For every annotated shot with a named shooter and
both caches, the possession logic picks the shooter on each, his boxes are cropped and read with
the jersey stack, and the vote speaks (who_eval.speak) with any number allowed, or only the
shooting team's roster. Shows whether a better tracker fixes the wrong-player errors.

Usage: who_sam3_eval.py [--out who_sam3.jsonl]   (box, CUDA env + jersey stack)
"""
import argparse
import json
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402
import who_eval as W  # noqa: E402

GAMES = {
    "cb9e1294": {"eval": "out_fast/eval_cb9e1294_vfast.json", "sam3": "out", "fast": "out_fast",
                 "clips": "/home/dev/confirm_test/clips/cb9e1294"},
    "7cef734e": {"eval": "out7/eval_7cef734e_v7.json", "sam3": "out7", "fast": "out7_fast",
                 "clips": "/home/dev/confirm_test/clips/7cef734e_sync"},
}
GT = json.load(open(os.path.join(HERE, "who_gt_dev.json")))   # game -> name -> [jersey, team]


def shooter_track(cache_dir, clip, cam):
    S = H.load(cache_dir, clip)
    res = H.analyse(S, H.Camera(cam))
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return None
    shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
    return (res.get("tracks") or {}).get(str(shot["shooter"])) or None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="who_sam3.jsonl")
    a = ap.parse_args()
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    done = set()
    if os.path.exists(a.out):
        done = {j["clip"] for j in map(json.loads, open(a.out))}
    with open(a.out, "a") as fo:
        for g, cfg in GAMES.items():
            roster = {}
            for name, (num, team) in GT[g].items():
                roster.setdefault(team, set()).add(str(num))
            for r in json.load(open(os.path.join(HERE, cfg["eval"])))["rows"]:
                clip = r["clip"]
                who = GT[g].get(r.get("gt_player") or "")
                if clip in done or who is None:
                    continue
                if not all(os.path.exists(os.path.join(HERE, cfg[k], "cache", clip + ".npz")) for k in ("sam3", "fast")):
                    continue
                cam = clip.split("_")[0]
                rec = {"game": g, "clip": clip, "gt": str(who[0]), "team_roster": sorted(roster[who[1]])}
                for k in ("fast", "sam3"):
                    try:
                        tr = shooter_track(os.path.join(HERE, cfg[k]), clip, cam)
                        rec[k] = W.read_track(js, os.path.join(cfg["clips"], clip + ".mp4"), tr) if tr else None
                    except Exception as e:  # noqa: BLE001
                        rec[k + "_err"] = "%s: %s" % (type(e).__name__, e)
                fo.write(json.dumps(rec) + "\n")
                fo.flush()
    summarize(a.out)


def summarize(path):
    rows = [json.loads(l) for l in open(path)]
    n = len(rows)
    print("[who3] %d shots" % n)
    for k in ("fast", "sam3"):
        for label, allowed in (("any number", lambda r: None), ("team roster", lambda r: set(r["team_roster"]))):
            ans = [W.speak(r.get(k) or [], allowed(r)) for r in rows]
            spoke = sum(x is not None for x in ans)
            right = sum(x == r["gt"] for x, r in zip(ans, rows))
            print("[who3] %-4s %-11s spoke %3d (%.0f%%)  right %3d  precision %.0f%%  right of all %.0f%%"
                  % (k, label, spoke, 100 * spoke / n, right, 100 * right / max(1, spoke), 100 * right / n))
    fa = [W.speak(r.get("fast") or [], None) for r in rows]
    sa = [W.speak(r.get("sam3") or [], None) for r in rows]
    c = Counter()
    for f, s, r in zip(fa, sa, rows):
        c["fast right, sam3 wrong/silent"] += f == r["gt"] and s != r["gt"]
        c["sam3 right, fast wrong/silent"] += s == r["gt"] and f != r["gt"]
        c["fast WRONG, sam3 right"] += f is not None and f != r["gt"] and s == r["gt"]
        c["present on sam3 track"] += any(x == r["gt"] for x, _ in (r.get("sam3") or []))
        c["present on fast track"] += any(x == r["gt"] for x, _ in (r.get("fast") or []))
    print("[who3] %s" % dict(c))


if __name__ == "__main__":
    main()

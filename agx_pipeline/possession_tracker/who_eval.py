"""WHO shot it: read the jersey number on the TRACKED shooter, scored against the annotators.

For every annotated shot of the validation games (who_gt.json: time, side, jersey, team):
  1. load the cached fast tracks of that shot's clip (err_review.py cached them) and run the
     possession logic -> the shooter and his box on every frame of the clip
  2. crop the shooter on every frame he is tracked, read the number with the jersey stack
     (legibility -> number localiser -> PARSeq), keep reads with confidence >= READ_MIN
  3. vote, weighted by read confidence, under three candidate sets:
       any        every number read
       game       numbers on either team's roster
       team       the shooting team's roster (from the annotator's card = the ceiling of a
                  perfect team call; in production the team comes from the side + period)
  4. speak only when the top number has >= V_MIN weight and beats the runner-up by V_RATIO

Usage: who_eval.py [--dump who_reads.jsonl]   (box, /home/dev/possession, CUDA env + jersey stack)
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")

import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402

VAL = "/home/dev/validate"
READ_MIN = 0.5
V_MIN, V_RATIO = 1.5, 1.5


def shooter_boxes(d, name, cam):
    S = H.load(d, name)
    res = H.analyse(S, H.Camera(cam))
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return None, None
    shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
    return (res.get("tracks") or {}).get(str(shot["shooter"]), []), shot


def read_track(js, clip, boxes):
    cap = cv2.VideoCapture(clip)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    want = {int(round(t * fps)): b for t, b in boxes}
    crops, i = [], 0
    while want:
        ok, im = cap.read()
        if not ok:
            break
        if i in want:
            b = [int(v) for v in want.pop(i)]
            h, w = im.shape[:2]
            x0, y0, x1, y1 = max(0, b[0]), max(0, b[1]), min(w, b[2]), min(h, b[3])
            if x1 - x0 > 8 and y1 - y0 > 16:
                crops.append(im[y0:y1, x0:x1].copy())
        i += 1
    cap.release()
    if not crops:
        return []
    reads = js.read_crops(crops)
    return [(str(n), float(c or 0)) for n, c in reads if n is not None]


def speak(reads, allowed):
    vote = Counter()
    for n, c in reads:
        if c >= READ_MIN and (allowed is None or n in allowed):
            vote[n] += c
    if not vote:
        return None
    top = vote.most_common(2)
    if top[0][1] >= V_MIN and (len(top) == 1 or top[0][1] >= V_RATIO * top[1][1]):
        return top[0][0]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="who_reads.jsonl")
    a = ap.parse_args()
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    G = json.load(open(os.path.join(HERE, "who_gt.json")))
    roster = defaultdict(set)
    team_roster = defaultdict(set)
    for r in G["rosters"]:
        roster[r["game"]].add(str(r["jersey"]))
        team_roster[(r["game"], r["team"])].add(str(r["jersey"]))
    done = set()
    if os.path.exists(a.dump):
        done = {(j["game"], j["t"]) for j in map(json.loads, open(a.dump))}
    with open(a.dump, "a") as fo:
        for s in G["shots"]:
            if (s["game"], s["t"]) in done:
                continue
            cam = "FL" if (s["side"] or "LEFT").upper() == "LEFT" else "FR"
            d = os.path.join(VAL, s["game"])
            name = "%s_t%07.1f" % (cam, s["t"])
            rec = {"game": s["game"], "t": s["t"], "cam": cam, "gt": str(s["jersey"]), "team": s["team"]}
            if not os.path.exists(os.path.join(d, "cache", name + ".npz")):
                rec["skip"] = "no cache"
            else:
                try:
                    boxes, shot = shooter_boxes(d, name, cam)
                    if boxes:
                        rec["n_frames"] = len(boxes)
                        rec["reads"] = read_track(js, os.path.join(d, "clips", name + ".mp4"), boxes)
                    else:
                        rec["skip"] = "no shooter"
                except Exception as e:  # noqa: BLE001
                    rec["skip"] = "%s: %s" % (type(e).__name__, e)
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
    summarize(a.dump, roster, team_roster)


def summarize(path, roster, team_roster):
    rows = [json.loads(l) for l in open(path)]
    ok = [r for r in rows if "reads" in r]
    print("[who] %d shots, %d with a tracked shooter and reads, skipped %s"
          % (len(rows), len(ok), dict(Counter(r.get("skip", "")[:20] for r in rows if "reads" not in r))))
    for label, allowed in (("any", lambda r: None), ("game roster", lambda r: roster[r["game"]]),
                           ("shooting-team roster", lambda r: team_roster[(r["game"], r["team"])])):
        spoke = right = 0
        for r in ok:
            a = speak(r["reads"], allowed(r))
            spoke += a is not None
            right += a == r["gt"]
        n = len(rows)
        print("[who] %-22s spoke %3d/%d (%.0f%%)  right %3d  precision %.0f%%  right of all %.0f%%"
              % (label, spoke, n, 100 * spoke / n, right, 100 * right / max(1, spoke), 100 * right / n))
    present = sum(1 for r in ok if any(n == r["gt"] for n, c in r["reads"]))
    print("[who] ceiling: true number read at least once on the tracked shooter: %d/%d" % (present, len(rows)))


if __name__ == "__main__":
    main()

"""Where do the shipped tracker's shot-type errors come from, on the UNSEEN validation games?

For every annotated shot (validate/<uuid>/results.jsonl) the shipped rule is re-run from the cached
fast tracks (holder base + baseline fix, no production fallback): shooter -> median takeoff feet ->
zone on production's golden arcs. Each shot is tagged:
  right / no answer / wrong shooter (the automatic shooter labels say the tracked player is not
  the annotated shooter) / right shooter, feet on the wrong side of a line
and for the last group: which way the error goes (deeper or shallower than the truth, in the
order 2PT < 3PT < 4PT) and how far the feet are from the line they would have to cross (px).
Alternative foot estimates from feet_study.rules are typed too, so a better foot point can be
scored on the same shots.

Usage: feet_errors.py [<out.jsonl>]   (box, /home/dev/possession)
"""
import json
import os
import sys
from collections import Counter

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import feet_study as F  # noqa: E402
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402

VAL = "/home/dev/validate"
GAMES = ["11850fc1-34c9-47ae-a6e0-6edde5ed0bc9", "0f58ef71-cda0-4096-a5c8-2467e0f95613",
         "302c9277-5fc7-4f3a-849d-daacd5ff7c76"]
ORDER = {"2PT": 0, "3PT": 1, "4PT": 2}


def line_dist(arcs, px):
    """Signed px distance to the 3PT (white) and 4PT (red) boundaries (+ inside)."""
    pt = (float(px[0]), float(px[1]))
    return {k: float(cv2.pointPolygonTest(arcs[k], pt, True)) for k in ("three_pt_white", "four_pt_red")}


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "feet_errors.jsonl"
    labels = {(r["game"], round(float(r["t"]), 1)): r for r in map(json.loads, open(os.path.join(HERE, "shooter_labels_auto.jsonl")))}
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    rows = []
    for g in GAMES:
        d = os.path.join(VAL, g)
        for gt in map(json.loads, open(os.path.join(d, "results.jsonl"))):
            cam, t = gt["cam"], float(gt["t"])
            name = "%s_t%07.1f" % (cam, t)
            rec = {"game": g, "t": t, "cam": cam, "gt": gt["gt"], "result": gt.get("result")}
            if not os.path.exists(os.path.join(d, "cache", name + ".npz")):
                rec["tag"] = "no cache"
                rows.append(rec)
                continue
            S = H.load(d, name)
            res = H.analyse(S, H.Camera(cam))
            shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
            if not shots:
                rec.update(tag="no answer", pred=None)
                rows.append(rec)
                continue
            shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
            track = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
            feet, still = T.takeoff_feet(track, shot["release_t"])
            if feet is None:
                rec.update(tag="no answer", pred=None)
                rows.append(rec)
                continue
            pred = T.zone_of(arcs[cam], feet, still)
            lab = labels.get((g, round(t, 1)))
            shooter_ok = None if not lab or not lab.get("label_tracks") else str(shot["shooter"]) in {str(x) for x in lab["label_tracks"]}
            alt, _ = F.rules(S, shot["shooter"], shot["release_t"])
            rec.update(pred=pred, feet=[round(v, 1) for v in feet], still=still, shooter=shot["shooter"],
                       shooter_ok=shooter_ok, dist=line_dist(arcs[cam], feet),
                       alt={k: T.zone_of(arcs[cam], v, still) for k, v in alt.items()})
            if pred == gt["gt"]:
                rec["tag"] = "right"
            elif shooter_ok is False:
                rec["tag"] = "wrong shooter"
            elif shooter_ok is None:
                rec["tag"] = "wrong, shooter unknown"
            else:
                rec["tag"] = "right shooter, wrong zone"
            if pred in ORDER and gt["gt"] in ORDER and pred != gt["gt"]:
                rec["dir"] = "deeper" if ORDER[pred] > ORDER[gt["gt"]] else "shallower"
            rows.append(rec)
    with open(out, "w") as fo:
        for r in rows:
            fo.write(json.dumps(r) + "\n")
    summarize(rows)


def summarize(rows):
    n = len(rows)
    print("[feet] %d shots: %s" % (n, Counter(r["tag"] for r in rows).most_common()))
    zw = [r for r in rows if r["tag"] == "right shooter, wrong zone"]
    print("[feet] right shooter, wrong zone: %s" % Counter("%s->%s" % (r["gt"], r["pred"]) for r in zw).most_common())
    print("[feet]   direction: %s" % Counter(r.get("dir", "FT-related") for r in zw).most_common())
    near = []
    for r in zw:
        if r.get("dir"):
            k = "three_pt_white" if {r["gt"], r["pred"]} & {"2PT"} else "four_pt_red"
            near.append(abs(r["dist"][k]))
    if near:
        print("[feet]   px from the line to cross: median %.0f, <=20: %d, <=40: %d, <=80: %d, of %d"
              % (np.median(near), sum(x <= 20 for x in near), sum(x <= 40 for x in near), sum(x <= 80 for x in near), len(near)))
    for k in ("box_med", "grounded", "mask_x", "grounded_mx"):
        ok = sum(1 for r in rows if r.get("alt", {}).get(k) == r["gt"])
        print("[feet] rule %-12s right %d/%d" % (k, ok, n))


if __name__ == "__main__":
    main()

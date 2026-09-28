"""Which foot-position rule types shots best? Offline, on cached tracks of GT-paired shots.

The tracker's typical miss is a 3 read as a 4 (feet too deep). Suspects: frames where the
shooter is already in the air (airborne feet project deeper), and the box centre being pulled
sideways by a raised shooting arm or the ball. Rules compared, all on production's golden arcs:

  box_med      median box bottom-centre over TAKEOFF_WIN before release (current rule)
  grounded     same, only frames whose box bottom is within GROUND_TOL of the lowest bottom
               in the window (feet still on the floor), after a percentile guard against
               merged boxes
  mask_x       box_med, but x from the lowest rows of the player's mask (the feet)
  grounded_mx  grounded + mask x

Usage: feet_study.py <cache_dir> <eval.json> <holder_ver>
e.g.   feet_study.py out_fast out_fast/eval_cb9e1294_vfast_golden.json vfast
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import holder as H  # noqa: E402

TAKEOFF_WIN = 0.8
GROUND_TOL = 0.04        # x box height
FT_STILL_PX = 60
ARCS = os.environ.get("ARCS_DIR", "/home/dev/shot_typing")


def arcs(cam):
    a = json.load(open(os.path.join(ARCS, "calib_arcs_%s.json" % cam)))
    sc = 1920 / float(a.get("w") or 1920)
    return {k: np.array(a[k], np.float32) * sc for k in ("three_pt_white", "four_pt_red", "ft_stripe") if a.get(k)}


A = {c: arcs(c) for c in ("FL", "FR")}


def zone(cam, px, still):
    pt = (float(px[0]), float(px[1]))
    a = A[cam]
    if still and "ft_stripe" in a and cv2.pointPolygonTest(a["ft_stripe"], pt, False) >= 0:
        return "FREE_THROW"
    if cv2.pointPolygonTest(a["three_pt_white"], pt, False) >= 0:
        return "2PT"
    return "3PT" if cv2.pointPolygonTest(a["four_pt_red"], pt, False) >= 0 else "4PT"


def mask_feet_x(mask, scale, box):
    """x of the feet: median column of the lowest 6% of the mask's rows (full-res px)."""
    ys, xs = np.nonzero(mask)
    if len(ys) < 20:
        return (box[0] + box[2]) / 2
    cut = np.percentile(ys, 94)
    return float(np.median(xs[ys >= cut])) / scale


def rules(S, sid, rt):
    t = S["times"]
    win = [k for k in range(len(t)) if rt - TAKEOFF_WIN <= t[k] <= rt and sid in S["players"][k]]
    if not win:
        return {}, False
    boxes = np.array([S["players"][k][sid]["box"] for k in win], float)
    bx = (boxes[:, 0] + boxes[:, 2]) / 2
    by = boxes[:, 3]
    h = np.median(boxes[:, 3] - boxes[:, 1])
    mx = np.array([mask_feet_x(S["players"][k][sid]["mask"], S["scale"], S["players"][k][sid]["box"]) for k in win])
    still = bool(np.ptp(bx) < FT_STILL_PX and np.ptp(by) < FT_STILL_PX)
    floor = np.percentile(by, 80)                   # the floor level, robust to one merged box
    g = by >= floor - GROUND_TOL * h
    return {"box_med": (np.median(bx), np.median(by)),
            "grounded": (np.median(bx[g]), np.median(by[g])),
            "mask_x": (np.median(mx), np.median(by)),
            "grounded_mx": (np.median(mx[g]), np.median(by[g]))}, still


def main():
    cache_dir, eval_path, ver = sys.argv[1:4]
    rows = json.load(open(eval_path))["rows"]
    tally, n, flips = {}, 0, {}
    for r in rows:
        if r.get("shooter") in (None, "NOT_RUN") or r.get("release_t") is None:
            continue
        p = os.path.join(cache_dir, "cache", r["clip"] + ".npz")
        if not os.path.exists(p):
            continue
        cam = r["clip"].split("_")[0]
        S = H.load(cache_dir, r["clip"])
        sid = r["shooter"]
        sid = int(sid) if not isinstance(sid, int) and str(sid).lstrip("-").isdigit() else sid
        fv, still = rules(S, sid, r["release_t"])
        if not fv:
            continue
        n += 1
        base = zone(cam, fv["box_med"], still) == r["gt"]
        for name, px in fv.items():
            ok = zone(cam, px, still) == r["gt"]
            tally[name] = tally.get(name, 0) + ok
            if name != "box_med" and ok != base:
                f = flips.setdefault(name, [0, 0])
                f[0 if ok else 1] += 1
    print("%s: %d shots" % (eval_path, n))
    for name, v in tally.items():
        fx = flips.get(name, [0, 0])
        print("  %-12s %3d/%d (%.1f%%)%s" % (name, v, n, 100 * v / n, "" if name == "box_med" else
                                           "   vs box_med: fixed %d, broke %d" % tuple(fx)))


if __name__ == "__main__":
    main()

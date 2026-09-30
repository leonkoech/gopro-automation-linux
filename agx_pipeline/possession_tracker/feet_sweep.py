"""Foot-point variants for shot type, scored on the development games AND the unseen games.

Same shipped chain as feet_errors.py (fast tracks -> holder -> shooter -> takeoff window), with
the foot point taken as:
  box      median box bottom-centre (shipped)
  maskx    same y, x from the lowest rows of the shooter's mask (a raised arm or the ball pulls
           the box centre sideways; the mask's bottom rows are the feet)
and each of those moved SHIFT cm towards the attacked basket on the court (the errors lean deep:
18 deeper vs 8 shallower on the unseen games), then typed on production's golden arcs.

Sets: dev cb9e1294 (out_fast), dev 7cef734e (out7_fast), unseen = the three validate/ games.
Usage: feet_sweep.py   (box, /home/dev/possession)
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import feet_study as F  # noqa: E402
import holder as H  # noqa: E402
import render_2k as R  # noqa: E402
import track_shadow as T  # noqa: E402

VAL = "/home/dev/validate"
UNSEEN = ["11850fc1-34c9-47ae-a6e0-6edde5ed0bc9", "0f58ef71-cda0-4096-a5c8-2467e0f95613",
          "302c9277-5fc7-4f3a-849d-daacd5ff7c76"]
SHIFTS = [-20, 0, 10, 20, 30, 45]
BASKET = {"FL": np.array([107.7, 713.2]), "FR": np.array([2036.0, 713.2])}


def shots_dev(cache_dir, eval_path):
    for r in json.load(open(eval_path))["rows"]:
        if os.path.exists(os.path.join(cache_dir, "cache", r["clip"] + ".npz")):
            yield cache_dir, r["clip"], r["clip"].split("_")[0], r["gt"]


def shots_unseen():
    for g in UNSEEN:
        d = os.path.join(VAL, g)
        for r in map(json.loads, open(os.path.join(d, "results.jsonl"))):
            name = "%s_t%07.1f" % (r["cam"], float(r["t"]))
            if os.path.exists(os.path.join(d, "cache", name + ".npz")):
                yield d, name, r["cam"], r["gt"]


def moved(cam_obj, cam, px, shift):
    if shift == 0:
        return px
    c = cam_obj.to_court([px])[0]
    v = BASKET[cam] - c
    c2 = c + shift * v / max(np.linalg.norm(v), 1e-6)
    img = cv2.perspectiveTransform(np.array([[c2]], np.float32), np.linalg.inv(cam_obj.H)).reshape(-1, 2)
    return R.redistort(cam_obj.cal, img)[0]


def variants(out, name, cam, arcs, cams):
    S = H.load(out, name)
    res = H.analyse(S, cams[cam])
    shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
    if not shots:
        return None
    shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
    track = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
    feet, still = T.takeoff_feet(track, shot["release_t"])
    alt, _ = F.rules(S, shot["shooter"], shot["release_t"])
    if feet is None or not alt:
        return None
    pts = {"box": feet, "maskx": [alt["mask_x"][0], feet[1]]}
    return {"%s%+d" % (k, s): T.zone_of(arcs[cam], moved(cams[cam], cam, p, s), still)
            for k, p in pts.items() for s in SHIFTS}


def main():
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    cams = {c: H.Camera(c) for c in ("FL", "FR")}
    sets = {"dev cb9e1294": shots_dev(os.path.join(HERE, "out_fast"), os.path.join(HERE, "out_fast", "eval_cb9e1294_vfast.json")),
            "dev 7cef734e": shots_dev(os.path.join(HERE, "out7_fast"), os.path.join(HERE, "out7", "eval_7cef734e_v7.json")),
            "unseen 3 games": shots_unseen()}
    for label, it in sets.items():
        rows = []
        for out, name, cam, gt in it:
            try:
                v = variants(out, name, cam, arcs, cams)
            except Exception as e:  # noqa: BLE001
                print("  skip", name, type(e).__name__, e, flush=True)
                continue
            rows.append((gt, v))
        n = len(rows)
        base = [v is not None and v["box+0"] == gt for gt, v in rows]
        print("[sweep] %s: %d shots, shipped (box+0) right %d" % (label, n, sum(base)), flush=True)
        keys = sorted({k for _, v in rows if v for k in v})
        for k in keys:
            ok = [v is not None and v[k] == gt for gt, v in rows]
            fx = sum(o and not b for o, b in zip(ok, base))
            br = sum(b and not o for o, b in zip(ok, base))
            print("[sweep]   %-9s right %3d  fixed %2d broke %2d  net %+d" % (k, sum(ok), fx, br, fx - br), flush=True)


if __name__ == "__main__":
    main()

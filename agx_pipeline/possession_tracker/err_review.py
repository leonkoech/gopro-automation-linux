"""Review sheets for validation shots the tracker typed wrong: re-track the clip (fast), keep
the cache, and draw, at three instants before release, the shooter's box, the median foot
point the typing used (red dot), and production's 3PT (cyan) / 4PT (magenta) lines.

Usage: err_review.py <uball_game_id>     (errors only unless REV_ALL=1; REV_NOIMG=1 skips sheets)
Writes /home/dev/validate/<uuid>/review/<t>_<gt>_as_<final>.jpg and keeps caches in
/home/dev/validate/<uuid>/cache/ for later foot-rule studies.
"""
import json
import os
import sys

import cv2
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import holder as H  # noqa: E402
import track_shadow as T  # noqa: E402


def main():
    g = sys.argv[1]
    d = "/home/dev/validate/" + g
    rows = [json.loads(l) for l in open(os.path.join(d, "results.jsonl"))]
    rows = [r for r in rows if "error" not in r and (os.environ.get("REV_ALL") == "1" or (r.get("final") or r.get("prod")) != r["gt"])]
    os.makedirs(os.path.join(d, "review"), exist_ok=True)
    clips = os.path.join(d, "clips")
    os.makedirs(clips, exist_ok=True)
    print("[rev] %s: %d shots" % (g[:8], len(rows)), flush=True)
    for r in rows:
        name = "%s_t%07.1f" % (r["cam"], r["t"])
        clip = os.path.join(clips, name + ".mp4")
        if not os.path.exists(clip) and not T.cut_clip(os.path.join(d, r["cam"] + ".mp4"), r["t"], clip):
            continue
        if not os.path.exists(os.path.join(d, "cache", name + ".npz")):
            os.system("python3 %s %s %s %.2f > /dev/null 2>&1" % (os.path.join(HERE, "perceive_fast.py"), clip, d, T.PRE))
        try:
            S = H.load(d, name)
            res = H.analyse(S, H.Camera(r["cam"]))
        except Exception as e:  # noqa: BLE001
            print("[rev]", name, "failed", e, flush=True)
            continue
        shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
        if not shots:
            continue
        shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
        sid, rt = shot["shooter"], shot["release_t"]
        arcs = T.load_arcs(r["cam"], 1920)
        track = [(t, b) for t, b in (res.get("tracks") or {}).get(str(sid), []) if rt - 0.8 <= t <= rt]
        feet, _ = T.takeoff_feet(res["tracks"][str(sid)], rt) if res.get("tracks") else (None, None)
        cap = cv2.VideoCapture(clip)
        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        tiles = []
        for dt in (-0.6, -0.3, 0.0):
            cap.set(cv2.CAP_PROP_POS_MSEC, (rt + dt) * 1000)
            ok, im = cap.read()
            if not ok:
                continue
            cv2.polylines(im, [arcs["three_pt_white"].astype(np.int32)], True, (255, 255, 0), 2)
            cv2.polylines(im, [arcs["four_pt_red"].astype(np.int32)], True, (255, 0, 255), 2)
            near = min(track, key=lambda tb: abs(tb[0] - (rt + dt)), default=None)
            if near:
                b = [int(v) for v in near[1]]
                cv2.rectangle(im, (b[0], b[1]), (b[2], b[3]), (0, 255, 0), 3)
            if feet:
                cv2.circle(im, (int(feet[0]), int(feet[1])), 9, (0, 0, 255), -1)
            cv2.putText(im, "t=rel%+.1fs  GT %s  prod %s  tracker %s  line %s px" % (
                dt, r["gt"], r.get("prod"), r.get("final"), r.get("line_px")), (20, 45),
                cv2.FONT_HERSHEY_SIMPLEX, 1.1, (0, 0, 0), 5)
            cv2.putText(im, "t=rel%+.1fs  GT %s  prod %s  tracker %s  line %s px" % (
                dt, r["gt"], r.get("prod"), r.get("final"), r.get("line_px")), (20, 45),
                cv2.FONT_HERSHEY_SIMPLEX, 1.1, (255, 255, 255), 2)
            tiles.append(cv2.resize(im, (960, 540)))
        cap.release()
        if tiles and os.environ.get("REV_NOIMG") != "1":
            out = os.path.join(d, "review", "%07.1f_%s_as_%s.jpg" % (r["t"], r["gt"], r.get("final")))
            cv2.imwrite(out, np.vstack(tiles), [cv2.IMWRITE_JPEG_QUALITY, 80])
    print("[rev] done", flush=True)


if __name__ == "__main__":
    main()

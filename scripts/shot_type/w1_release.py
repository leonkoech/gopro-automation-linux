"""W1: extract the RELEASE time from SL/SR footage for the 28 GT shots (game 0d96e12a).

Per shot: decode a short SL/SR window around the GT (outcome) ts, build the 120fps ball
track (deployed v3 detector), find the rim-crossing (deployed logic.decide), then WALK BACK
along the track to the flight start = release (or FOV-entry for deep shooters). Emit
rim/release in base coords so W2 can fit the mapping and W3 can anchor agx_classify.

Run on the AGX:  cd /home/dev/gopro-automation-linux && python3 w1_release.py
Output: one 'REL' line per shot + a summary; also /home/dev/scratch_shot_timing/w1_release.json
"""
import json
import os
import sys

import numpy as np

sys.path.insert(0, "/home/dev/gopro-automation-linux")
os.environ.setdefault("SHOT_DET_IMGSZ", "640")          # deployed production setting
from agx_pipeline.shot_detect import logic              # noqa: E402
from agx_pipeline.shot_detect.detect import ShotDetector, iter_frames  # noqa: E402

REC = "/home/dev/app/recordings/game_20260811_004039"
WEIGHTS = "/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/weights/ball_yolo26s_gray_hifps_v3_best.pt"
RIMS = json.load(open("/home/dev/gopro-automation-linux/agx_pipeline/shot_detect/rims.json"))
FPS = 120.0
PRE_S, POST_S = 5.5, 2.5                                 # window around the GT outcome ts

# (side, base_ts, gt_type) — LEFT hoop -> SL cam, RIGHT -> SR. 28 human-GT shots.
GT = [("SL", 216.0, "4PT"), ("SL", 297.0, "3PT"), ("SL", 370.5, "4PT"), ("SL", 570.2, "4PT"),
      ("SL", 643.0, "4PT"), ("SL", 1005.9, "4PT"), ("SL", 1050.4, "3PT"), ("SL", 1213.8, "4PT"),
      ("SL", 1356.3, "4PT"), ("SL", 1433.9, "3PT"),
      ("SR", 205.0, "4PT"), ("SR", 236.1, "3PT"), ("SR", 389.4, "4PT"), ("SR", 479.2, "3PT"),
      ("SR", 510.3, "3PT"), ("SR", 515.8, "3PT"), ("SR", 556.7, "3PT"), ("SR", 561.7, "4PT"),
      ("SR", 650.1, "4PT"), ("SR", 708.3, "4PT"), ("SR", 730.8, "3PT"), ("SR", 787.2, "3PT"),
      ("SR", 794.7, "3PT"), ("SR", 886.4, "4PT"), ("SR", 947.0, "4PT"), ("SR", 1204.9, "4PT"),
      ("SR", 1237.4, "3PT"), ("SR", 1415.9, "3PT")]

GAP_S = 0.35          # max track gap inside one flight
RHO_TOL = 0.6         # rho^2 tolerance for the monotonic-approach test
V_FLIGHT = 1.2        # px/frame @120fps — slower than this = ball in hands, stop
MAX_FLIGHT_S = 2.5


def release_walk_back(G, track, cross_frame):
    """From the rim-cross, walk back along the track keeping the incoming flight:
    small gaps, rho increasing as we go back (ball farther from rim), flight-speed
    motion. First kept point = flight start (release, or FOV-entry for deep shots)."""
    pts = sorted([t for t in track if t[0] <= cross_frame], key=lambda t: t[0])
    if not pts:
        return None, 0
    kept = [pts[-1]]
    for t in reversed(pts[:-1]):
        nxt = kept[-1]
        if (nxt[0] - t[0]) / FPS > GAP_S:
            break
        if (cross_frame - t[0]) / FPS > MAX_FLIGHT_S:
            break
        if G.rho(t[1], t[2]) < G.rho(nxt[1], nxt[2]) - RHO_TOL:
            break                                        # approach broken (going back, rho must rise)
        v = float(np.hypot(nxt[1] - t[1], nxt[2] - t[2])) / max(1, nxt[0] - t[0])
        if v < V_FLIGHT:
            break                                        # slow = in hands
        kept.append(t)
    return kept[-1][0], len(kept)


det = ShotDetector(WEIGHTS)
rows = []
for side, ts, gtt in GT:
    video = f"{REC}/game_20260811_004039_{side}.mp4"
    ss = max(0.0, ts - PRE_S)
    n = int((PRE_S + POST_S) * FPS)
    track, _ = det._ball_track_stream(iter_frames(video, ss=ss, max_frames=n))
    G = logic.Geo.from_rim(RIMS[side], FPS)
    verdicts = [v for v in logic.decide(G, track) if "verdict" in v]
    if not verdicts:
        print(f"REL {side} {ts} {gtt} :: NO_EVENT ntrack={len(track)}", flush=True)
        rows.append(dict(side=side, gt_ts=ts, gt_type=gtt, ok=False, ntrack=len(track)))
        continue
    exp_local = PRE_S * FPS                              # the GT ts position inside the window
    prim = min(verdicts, key=lambda d: abs(d["cross_frame"] - exp_local))
    rel_f, nkept = release_walk_back(G, track, prim["cross_frame"])
    rim_base = ss + prim["cross_frame"] / FPS
    rel_base = ss + rel_f / FPS if rel_f is not None else None
    flight = (prim["cross_frame"] - rel_f) / FPS if rel_f is not None else None
    print(f"REL {side} {ts} {gtt} :: verdict={prim['verdict']} rim_base={rim_base:.2f} "
          f"rel_base={None if rel_base is None else round(rel_base, 2)} "
          f"flight={None if flight is None else round(flight, 2)}s "
          f"d_rim={rim_base - ts:+.2f} nkept={nkept} ntrack={len(track)}", flush=True)
    rows.append(dict(side=side, gt_ts=ts, gt_type=gtt, ok=True, verdict=prim["verdict"],
                     rim_base=round(rim_base, 3),
                     rel_base=None if rel_base is None else round(rel_base, 3),
                     flight_s=None if flight is None else round(flight, 3),
                     nkept=nkept, ntrack=len(track)))

good = [r for r in rows if r.get("ok")]
d = [r["rim_base"] - r["gt_ts"] for r in good]
fl = [r["flight_s"] for r in good if r.get("flight_s")]
print(f"\nSUMMARY: events {len(good)}/{len(rows)} | d_rim median {np.median(d):+.2f}s "
      f"(IQR {np.percentile(d, 25):+.2f}..{np.percentile(d, 75):+.2f}) | "
      f"flight median {np.median(fl):.2f}s (n={len(fl)})", flush=True)
json.dump(rows, open("/home/dev/scratch_shot_timing/w1_release.json", "w"), indent=1)
print("W1_DONE", flush=True)

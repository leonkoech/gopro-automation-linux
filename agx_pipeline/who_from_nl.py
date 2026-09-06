"""Standalone WHO from an NL clip window (ships to the box next to
agx_classify.py; run from SHOT_TYPING_CWD with the same env recipe).

    python3 who_from_nl.py <nl_clip.mp4> <rim_ts_in_clip_s> <tag>

Dense-tracks the window (~every 2nd frame), finds the last persistent ball
holder before the rim moment, and votes his jersey number: confidence-weighted
reads, optionally roster-filtered (WHO_ROSTER="1,7,8,23"), a clear-margin rule
-- prints `WHO_NL=#<num> conf=<w>` only when the vote is unambiguous, else
`WHO_NL=#None`. Silence over guessing, same as SHOT_TYPE_STRICT.

Method fleet-measured 2026-09-06 on 3 annotated games: 84% correct answers on
the clean game, unknown-heavy on games with illegible or SHARED numbers (two
teams both fielding #8 is unresolvable by numbers -- ops should avoid it).
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter

import cv2
import numpy as np

sys.path.insert(0, "src")
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from ultralytics import YOLO  # noqa: E402
from uball_cc.tracking.jersey_stack import JerseyStack  # noqa: E402

CLIP, RIM_TS, TAG = sys.argv[1], float(sys.argv[2]), sys.argv[3]
DEVICE = os.getenv("SHOT_DEVICE", "cuda")
ROSTER = {n.strip() for n in os.getenv("WHO_ROSTER", "").split(",") if n.strip()}

det = YOLO(os.getenv("WHO_NL_WEIGHTS", "unified_yolo26s.pt"))
js = JerseyStack()


def _iou(a, b):
    x1, y1 = max(a[0], b[0]), max(a[1], b[1])
    x2, y2 = min(a[2], b[2]), min(a[3], b[3])
    if x2 <= x1 or y2 <= y1:
        return 0.0
    i = (x2 - x1) * (y2 - y1)
    return i / ((a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - i)


cap = cv2.VideoCapture(CLIP)
fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
tracks, nid = {}, 1
closed = []
f = 0
while True:
    ok = cap.grab()
    if not ok:
        break
    f += 1
    if f % 2:
        continue
    ok, img = cap.retrieve()
    if not ok:
        break
    t = f / fps
    r = det.predict(img, imgsz=1280, conf=0.35, device=DEVICE, verbose=False)[0]
    if r.boxes is None:
        continue
    bx = r.boxes.xyxy.cpu().numpy()
    cl = r.boxes.cls.cpu().numpy()
    cf = r.boxes.conf.cpu().numpy()
    players = [b for b, k in zip(bx, cl) if int(k) == 0]
    balls = [b for b, k, c in zip(bx, cl, cf) if int(k) == 2 and c >= 0.25]
    bcx = bcy = None
    if balls:
        bb = balls[0]
        bcx, bcy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
    for tid in [tid for tid, tr in tracks.items() if t - tr["lt"] > 0.8]:
        closed.append(tracks.pop(tid))
    used = set()
    for tid, tr in tracks.items():
        best, bj = 0.3, -1
        for j, b in enumerate(players):
            if j in used:
                continue
            v = _iou(tr["last"], b)
            if v > best:
                best, bj = v, j
        if bj >= 0:
            b = players[bj]
            holds = bool(bcx is not None and b[0] <= bcx <= b[2] and b[1] <= bcy <= b[3])
            tr["last"], tr["lt"] = b, t
            tr["obs"].append((t, holds))
            if len(tr["obs"]) % 6 == 0:
                crop = img[int(b[1]):int(b[3]), int(b[0]):int(b[2])]
                if crop.size:
                    nn, cc = js.read_crop(crop)
                    if nn is not None:
                        w = float(cc) if cc is not None else 0.5
                        tr["nums"][str(nn)] = tr["nums"].get(str(nn), 0.0) + w
            used.add(bj)
    for j, b in enumerate(players):
        if j in used:
            continue
        holds = bool(bcx is not None and b[0] <= bcx <= b[2] and b[1] <= bcy <= b[3])
        tracks[nid] = {"last": b, "lt": t, "nums": {}, "obs": [(t, holds)]}
        nid += 1
closed.extend(tracks.values())

best = None                        # (hold_end_t, track)
for tr in closed:
    run, run_end = 0, None
    for t, holds in tr["obs"]:
        if t > RIM_TS - 0.2:
            break
        if holds:
            run += 1
            if run >= 2:
                run_end = t
        else:
            run = 0
    if run_end is not None and (best is None or run_end > best[0]):
        best = (run_end, tr)

who, conf = None, 0.0
if best is not None:
    nums = {k: v for k, v in best[1]["nums"].items()
            if not ROSTER or k in ROSTER}
    srt = sorted(nums.items(), key=lambda kv: -kv[1])
    if srt and srt[0][1] >= 1.2 and (len(srt) == 1 or srt[0][1] >= 1.6 * srt[1][1]):
        who, conf = srt[0][0], srt[0][1]
print(f"WHO_NL={TAG}: WHO_NL=#{who} conf={conf:.2f} tracks={len(closed)}", flush=True)

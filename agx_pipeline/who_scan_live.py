#!/usr/bin/env python3
"""WHO scan (production port of the validated eval design, 2026-09-08).

Given the FL and NL windows around a shot, the release feet from the typing
stack, and the game roster: seed the shooter in FL by feet, IoU-track that one
player outward (forward then backward, stop the moment the track is lost —
never guess), OCR every crop in BOTH views (FL sees jersey backs, NL fronts),
and speak only on a dominant vote. Measured on 150 GT shots: ~80% correct when
spoken, every game >=75%; silence otherwise.

Runs inside the classify working dir (same as agx_classify.py — the unified
detector weights and jersey weights resolve from there).

Usage:
  python3 who_scan_live.py <fl_clip> <nl_clip|-> <rim_in_fl_s> <feet_x> <feet_y> <tag>

Env:
  SHOT_WHO_ROSTER   comma-separated jersey numbers for the shooting team
                    (from check-in; empty -> accept any read)
  SHOT_WHO_WIN      scan half-window seconds (default 2.5)
  SHOT_WHO_STEP     sampling step seconds (default 0.15)
  SHOT_WHO_NL_OFF   NL clock offset vs FL seconds (default 0)
  SHOT_WHO_MIN      vote floor to speak (default 3.0)
  SHOT_WHO_RATIO    top-vs-runner-up ratio to speak (default 2.5)
  SHOT_DEVICE       cuda / mps / cpu

Prints:  WHO_SCAN=#<number> conf=<top_vote>   (or WHO_SCAN=#None conf=0)
"""
from __future__ import annotations

import os
import sys
from collections import Counter

import cv2
import numpy as np

FL_CLIP, NL_CLIP, RIM_S = sys.argv[1], sys.argv[2], float(sys.argv[3])
FEET = (float(sys.argv[4]), float(sys.argv[5]))
TAG = sys.argv[6] if len(sys.argv) > 6 else "shot"

WIN = float(os.getenv("SHOT_WHO_WIN", "2.5"))
STEP = float(os.getenv("SHOT_WHO_STEP", "0.15"))
NL_OFF = float(os.getenv("SHOT_WHO_NL_OFF", "0") or 0)
V_MIN = float(os.getenv("SHOT_WHO_MIN", "3.0"))
V_RATIO = float(os.getenv("SHOT_WHO_RATIO", "2.5"))
DEV = os.getenv("SHOT_DEVICE", "cuda")
ROSTER = {n.strip() for n in os.getenv("SHOT_WHO_ROSTER", "").split(",") if n.strip()}

from ultralytics import YOLO  # noqa: E402
from uball_cc.tracking.jersey_stack import JerseyStack  # noqa: E402

det = YOLO(os.getenv("SHOT_WHO_DET", "unified_yolo26s.pt"))
js = JerseyStack()


def players_at(cap, t):
    cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, t * 1000))
    ok, img = cap.read()
    if not ok:
        return None, []
    r = det.predict(img, imgsz=1280, conf=0.35, device=DEV, verbose=False)[0]
    boxes = [[float(v) for v in b] for b, k in
             zip(r.boxes.xyxy.cpu().numpy(), r.boxes.cls.cpu().numpy().astype(int))
             if k == 0] if r.boxes is not None else []
    return img, boxes


def iou(a, b):
    ix1, iy1 = max(a[0], b[0]), max(a[1], b[1])
    ix2, iy2 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0, ix2 - ix1) * max(0, iy2 - iy1)
    ua = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def add_read(img, b, vote):
    crop = img[int(b[1]):int(b[3]), int(b[0]):int(b[2])]
    if crop.size:
        nn, cc = js.read_crop(crop)
        if nn is not None and (not ROSTER or str(nn) in ROSTER):
            vote[str(nn)] += float(cc or 1.0)


def spoken(vote):
    if not vote:
        return None
    top = vote.most_common(2)
    if top[0][1] >= V_MIN and (len(top) == 1 or top[0][1] >= V_RATIO * top[1][1]):
        return top[0]
    return None


def track_and_read(cap, seed_box, t_seed, vote):
    for direction in (1, -1):
        box, t = seed_box, t_seed
        while abs(t - t_seed) <= WIN:
            t += direction * STEP
            img, boxes = players_at(cap, t)
            if img is None or not boxes:
                break
            best, bi = 0.0, None
            for i, b in enumerate(boxes):
                v = iou(box, b)
                if v > best:
                    best, bi = v, i
            if bi is None or best < 0.25:
                break                          # lost him: stop, never guess
            box = boxes[bi]
            add_read(img, box, vote)
            if spoken(vote):
                return


vote: Counter = Counter()
cap_fl = cv2.VideoCapture(FL_CLIP)
seed, seed_t = None, None
for dt in (-1.0, -1.4, -0.6):
    img, boxes = players_at(cap_fl, RIM_S + dt)
    if img is None or not boxes:
        continue
    d = [np.hypot((b[0]+b[2])/2 - FEET[0], b[3] - FEET[1]) for b in boxes]
    j = int(np.argmin(d))
    if d[j] < 180:
        seed, seed_t = boxes[j], RIM_S + dt
        add_read(img, seed, vote)
        break
if seed is not None and not spoken(vote):
    track_and_read(cap_fl, seed, seed_t, vote)
cap_fl.release()

if NL_CLIP != "-" and not spoken(vote) and seed is not None:
    # NL side: the shooter's NL box is found by horizontal proximity after
    # projecting is unavailable live; use the widest-coverage fallback — track
    # from the NL box nearest in normalized x to the FL seed at the same instant.
    cap_nl = cv2.VideoCapture(NL_CLIP)
    img, boxes = players_at(cap_nl, RIM_S + NL_OFF - 1.0)
    if img is not None and boxes:
        fx = (seed[0] + seed[2]) / 2 / 3840.0
        d = [abs(((b[0]+b[2])/2) / max(img.shape[1], 1) - fx) for b in boxes]
        j = int(np.argmin(d))
        if d[j] < 0.18:
            add_read(img, boxes[j], vote)
            if not spoken(vote):
                track_and_read(cap_nl, boxes[j], RIM_S + NL_OFF - 1.0, vote)
    cap_nl.release()

top = spoken(vote)
if top:
    print(f"WHO_SCAN=#{top[0]} conf={top[1]:.2f}")
else:
    print("WHO_SCAN=#None conf=0")

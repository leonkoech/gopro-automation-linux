"""Clock map between the SL/SR shot cameras and the FL/FR video of one game, WITHOUT ground truth.

The replay detects shots on the SL/SR masters (their own clock); clips must be cut from the FL/FR
masters (another clock, offset by ~10 s and drifting a few tenths of a percent). For a sample of
detected shots, the FL/FR video (the camera on the shot's side) is scanned SCAN_S either side of
the SL/SR time with the typing stage's ball+hoop detector, and every ball-at-hoop moment is a
candidate. One straight line video_t = a + b * master_t is fitted with RANSAC over all candidates:
the true pairs line up, the wrong ones (another shot at the same hoop) do not.

Usage: sync_masters.py <replay.json> <FL.mp4> <FR.mp4> <out.json> [n_sample=40]
"""
import json
import random
import sys

import cv2
import numpy as np

W = "/home/dev/shot_typing/yolo26s_ball_hoop_ft_evalweek_v1.pt"
SCAN_S = 15.0
FPS = 10.0
TOL_S = 0.4
B_RANGE = (0.99, 1.01)


def rim_moments(model, video, t0, t1):
    """Times (video s) where a detected ball sits at the detected hoop."""
    cap = cv2.VideoCapture(video)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    step = max(1, int(round(fps / FPS)))
    cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, t0) * 1000)
    i0 = int(round(max(0.0, t0) * fps))
    frames, times, i = [], [], 0
    while True:
        ok, im = cap.read()
        if not ok:
            break
        t = (i0 + i) / fps
        if t > t1:
            break
        if i % step == 0:
            frames.append(im)
            times.append(t)
        i += 1
    cap.release()
    hits = []
    for j in range(0, len(frames), 16):
        for k, r in enumerate(model.predict(frames[j:j + 16], imgsz=1280, conf=0.15, verbose=False)):
            cls = r.boxes.cls.cpu().numpy().astype(int)
            xy = r.boxes.xyxy.cpu().numpy()
            cf = r.boxes.conf.cpu().numpy()
            hoops = [(c, b) for c, b, q in zip(cf, xy, cls) if q == 1]
            balls = [b for b, q in zip(xy, cls) if q == 0]
            if not hoops or not balls:
                continue
            h = max(hoops, key=lambda x: x[0])[1]
            hw, hh = h[2] - h[0], h[3] - h[1]
            for b in balls:
                u, v = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
                if h[0] - 0.3 * hw <= u <= h[2] + 0.3 * hw and h[1] - hh <= v <= h[3] + hh:
                    hits.append(times[j + k])
                    break
    events, last = [], None                  # collapse consecutive hits into one moment
    for t in hits:
        if last is None or t - last > 0.6:
            events.append([t])
        else:
            events[-1].append(t)
        last = t
    return [float(np.median(e)) for e in events]


def ransac(pairs, iters=4000):
    best, best_in = None, []
    x = np.array([p[0] for p in pairs])
    y = np.array([p[1] for p in pairs])
    rng = random.Random(0)
    for _ in range(iters):
        i, j = rng.sample(range(len(pairs)), 2)
        if abs(x[i] - x[j]) < 300:
            continue
        b = (y[j] - y[i]) / (x[j] - x[i])
        if not B_RANGE[0] <= b <= B_RANGE[1]:
            continue
        a = y[i] - b * x[i]
        inl = np.abs(y - (a + b * x)) <= TOL_S
        if inl.sum() > len(best_in):
            best, best_in = (a, b), np.where(inl)[0]
    if best is None:
        return None
    b, a = np.polyfit(x[best_in], y[best_in], 1)
    res = np.abs(y[best_in] - (a + b * x[best_in]))
    return {"a": float(a), "b": float(b), "inliers": int(len(best_in)), "of": len(pairs),
            "resid_median_s": float(np.median(res))}


def main():
    replay, fl, fr, out = sys.argv[1:5]
    n = int(sys.argv[5]) if len(sys.argv) > 5 else 40
    from ultralytics import YOLO
    model = YOLO(W)
    R = json.load(open(replay))
    maps = {}
    for side, video, cam in (("SL", fl, "FL"), ("SR", fr, "FR")):
        shots = sorted((R.get(side) or {}).get("base", []) + (R.get(side) or {}).get("confirm_new", []),
                       key=lambda s: s["t"])
        pick = shots[::max(1, len(shots) // n)][:n]
        pairs = []
        for s in pick:
            for v in rim_moments(model, video, s["t"] - SCAN_S, s["t"] + SCAN_S + 15):
                pairs.append((float(s["t"]), v))
        maps[cam] = ransac(pairs) if len(pairs) >= 4 else None
        print(cam, len(pick), "shots,", len(pairs), "candidates ->", maps[cam], flush=True)
    json.dump(maps, open(out, "w"), indent=1)


if __name__ == "__main__":
    main()

"""Find ball-at-rim moments in a wide FL/FR view, timestamped in real seconds.

This is the independent half of the sync check. SL/SR give a trigger time from
the near-rim view; this gives one from the tracking camera, using a different
model on different footage. If the two agree, the cameras are synced and the
pairing is confirmed. If they differ by a constant, that constant is the offset
we have been missing. Neither can be established from the CV metadata alone,
which is why the earlier attempts kept going in circles.

Model is the YOLO26 ball+hoop specialist trained for this venue. Its handoff
notes are specific and are followed here rather than guessed at:

  - imgsz=1280 at inference. It was trained at 1280 on 1920x1080; at the default
    640 the ball is 12-20 px and effectively invisible.
  - conf=0.15 for the ball, best-per-frame.
  - ball coverage is only 47-74% of frames, so nothing may assume a continuous
    track -- the rim test is per-frame proximity, and events are grouped by time
    rather than by an unbroken trajectory.

The hoop barely moves, so it is fixed once from a robust median over the window
instead of being re-detected per frame; that keeps a single bad hoop box from
inventing an event. Proximity is scaled by hoop width, so the threshold means
the same thing on either camera regardless of how far away the rim sits.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPECIALIST = HERE / "weights" / "yolo26s_ball_hoop_specialist.pt"

IMGSZ = 1280           # non-negotiable per the handoff notes
BALL_CONF = 0.15
HOOP_CONF = 0.25
NEAR_RIM = 1.6         # ball within this many hoop-widths of the hoop centre
GROUP_S = 1.5          # detections closer than this in time are one event


def measured_fps(path: Path) -> tuple:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=nb_frames,duration", "-of", "json", str(path)],
        capture_output=True, text=True)
    s = json.loads(r.stdout)["streams"][0]
    nb, dur = float(s["nb_frames"]), float(s["duration"])
    return nb / dur, nb, dur


def median(v):
    v = sorted(v)
    return v[len(v) // 2] if v else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--video", required=True)
    ap.add_argument("--angle", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", type=float, default=0.0)
    ap.add_argument("--end", type=float, required=True)
    ap.add_argument("--stride", type=int, default=2, help="frame stride (2 => ~15fps)")
    a = ap.parse_args()

    import cv2
    from ultralytics import YOLO

    video = Path(a.video)
    fps, nframes, dur = measured_fps(video)
    print(f"{a.angle}: {nframes:.0f} frames / {dur:.2f}s -> MEASURED {fps:.4f} fps", flush=True)

    model = YOLO(str(SPECIALIST))
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        print("  cannot open video")
        return 1

    f0 = int(a.start * fps)
    f1 = int(min(a.end, dur) * fps)
    cap.set(cv2.CAP_PROP_POS_FRAMES, f0)

    hoops, dets = [], []
    fi = f0
    processed = 0
    while fi < f1:
        ok, frame = cap.read()
        if not ok:
            break
        if (fi - f0) % a.stride == 0:
            r = model.predict(frame, imgsz=IMGSZ, conf=BALL_CONF, verbose=False)[0]
            best_ball, best_hoop = None, None
            for b in r.boxes:
                cls = int(b.cls[0])
                conf = float(b.conf[0])
                x1, y1, x2, y2 = [float(v) for v in b.xyxy[0]]
                cx, cy, w = (x1 + x2) / 2, (y1 + y2) / 2, x2 - x1
                if cls == 0 and (best_ball is None or conf > best_ball[3]):
                    best_ball = (cx, cy, w, conf)
                elif cls == 1 and conf >= HOOP_CONF and (best_hoop is None or conf > best_hoop[3]):
                    best_hoop = (cx, cy, w, conf)
            if best_hoop:
                hoops.append(best_hoop)
            if best_ball:
                dets.append((fi / fps, best_ball))
            processed += 1
            if processed % 400 == 0:
                print(f"  {fi/fps:7.1f}s   ball dets {len(dets)}  hoop dets {len(hoops)}",
                      flush=True)
        fi += 1
    cap.release()

    if not hoops:
        print("  NO HOOP DETECTED -- cannot judge rim proximity")
        return 1
    hx = median([h[0] for h in hoops])
    hy = median([h[1] for h in hoops])
    hw = median([h[2] for h in hoops]) or 1.0
    print(f"  hoop fixed at ({hx:.0f},{hy:.0f}) width {hw:.0f}px "
          f"from {len(hoops)} detections", flush=True)

    near = []
    for t, (cx, cy, w, conf) in dets:
        d = ((cx - hx) ** 2 + (cy - hy) ** 2) ** 0.5 / hw
        if d <= NEAR_RIM:
            near.append({"t": round(t, 3), "d_hoopw": round(d, 3), "conf": round(conf, 3)})

    near.sort(key=lambda r: r["t"])
    events = []
    cur = []
    for r in near:
        if cur and r["t"] - cur[-1]["t"] > GROUP_S:
            events.append(cur); cur = []
        cur.append(r)
    if cur:
        events.append(cur)

    out = []
    for grp in events:
        best = min(grp, key=lambda r: r["d_hoopw"])     # closest approach = the rim moment
        out.append({"angle": a.angle, "real_t": best["t"], "min_d_hoopw": best["d_hoopw"],
                    "n_frames": len(grp),
                    "span": [grp[0]["t"], grp[-1]["t"]]})

    Path(a.out).write_text(json.dumps(
        {"angle": a.angle, "video": str(video), "measured_fps": fps, "duration": dur,
         "hoop": {"cx": hx, "cy": hy, "w": hw},
         "n_ball_dets": len(dets), "n_near": len(near), "events": out}, indent=1))
    print(f"\n{a.angle}: {len(dets)} ball detections, {len(near)} near-rim frames, "
          f"{len(out)} rim events -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

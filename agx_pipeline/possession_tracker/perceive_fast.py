"""Fast perception: the same cache as perceive.py, with SAM3 replaced by a YOLO segmentation
model + ByteTrack. Players get an outline and a stable id per frame at detector speed, which
is what the live pipeline can afford. holder.py and eval_full.py run on it unchanged, so the
comparison with the SAM3 caches is exact: same shots, same rules, only the perception differs.

Usage: perceive_fast.py <clip.mp4> <out_dir> [rim_t=5.0]
Env:   SEG_W (default yolo11s-seg.pt next to this file under weights/), BALL_W, SEG_IMGSZ (1280)
"""
import os
import sys
import time

import cv2
import numpy as np
import torch

HERE = os.path.dirname(os.path.abspath(__file__))
SEG_W = os.environ.get("SEG_W", os.path.join(HERE, "weights", "yolo11s-seg.pt"))
BALL_W = os.environ.get("BALL_W", "/home/dev/shot_typing/yolo26s_ball_hoop_ft_evalweek_v1.pt")
SEG_IMGSZ = int(os.environ.get("SEG_IMGSZ", "1280"))
WIN_BEFORE, WIN_AFTER = 4.0, 2.0
FPS_OUT = 15
MASK_W, MASK_H = 512, 288          # the SAM3 cache layout: 1024-wide frames, masks at half that
DEV = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")


def load_frames(clip, rim_t):
    cap = cv2.VideoCapture(clip)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    t0, t1 = rim_t - WIN_BEFORE, rim_t + WIN_AFTER
    step = max(1, int(round(fps / FPS_OUT)))
    full, times, i = [], [], 0
    while True:
        ok, f = cap.read()
        if not ok:
            break
        t = i / fps
        if t > t1 + 0.1:
            break
        if t0 <= t <= t1 and i % step == 0:
            full.append(f)
            times.append(t)
        i += 1
    cap.release()
    return full, np.array(times)


def players(full):
    """Person outlines + ByteTrack ids, frame by frame (persist=True carries the tracker)."""
    from ultralytics import YOLO
    m = YOLO(SEG_W)
    rows, masks = [], []
    for i, f in enumerate(full):
        r = m.track(f, persist=True, tracker="bytetrack.yaml", classes=[0], imgsz=SEG_IMGSZ,
                    conf=0.25, verbose=False, device=DEV, retina_masks=False)[0]
        if r.boxes is None or r.boxes.id is None or r.masks is None:
            continue
        ids = r.boxes.id.int().cpu().tolist()
        bx = r.boxes.xyxy.cpu().numpy()
        sc = r.boxes.conf.cpu().numpy()
        md = r.masks.data.cpu().numpy() > 0.5
        for j, oid in enumerate(ids):
            rows.append((i, oid, float(sc[j]), *[float(v) for v in bx[j]]))
            mk = cv2.resize(md[j].astype(np.uint8), (MASK_W, MASK_H), interpolation=cv2.INTER_NEAREST) > 0
            masks.append(np.packbits(mk))
    return np.array(rows, float).reshape(-1, 7), np.array(masks)


def detections(full):
    from ultralytics import YOLO
    m = YOLO(BALL_W)
    rows = []
    for i in range(0, len(full), 8):
        for k, r in enumerate(m.predict(full[i:i + 8], imgsz=1280, conf=0.05, verbose=False, device=DEV)):
            for b in r.boxes:
                rows.append((i + k, int(b.cls.item()), float(b.conf.item()), *[float(v) for v in b.xyxy[0]]))
    return np.array(rows, float).reshape(-1, 7)


def main():
    clip, out = sys.argv[1:3]
    rim_t = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0
    name = os.path.splitext(os.path.basename(clip))[0]
    os.makedirs(os.path.join(out, "cache"), exist_ok=True)
    t0 = time.time()
    full, times = load_frames(clip, rim_t)
    t_load = time.time()
    prow, pmask = players(full)
    t_seg = time.time()
    dets = detections(full)
    t_det = time.time()
    np.savez_compressed(os.path.join(out, "cache", name + ".npz"), times=times, rim_t=rim_t,
                        players=prow, masks=pmask, mask_shape=(MASK_H, MASK_W), dets=dets,
                        frame_shape=full[0].shape[:2], sam_w=MASK_W * 2, mask_ds=2)
    print(name, "frames", len(full), "player rows", len(prow), "dets", len(dets),
          "load %.0fs seg+track %.0fs ball %.0fs total %.0fs" % (t_load - t0, t_seg - t_load, t_det - t_seg,
                                                                  time.time() - t0), flush=True)


if __name__ == "__main__":
    main()

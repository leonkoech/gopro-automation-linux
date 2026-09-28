"""Perception pass, run once per clip and cached: everything slow lives here so the
possession logic (holder.py) can be iterated on in seconds.

Per clip, frames from WIN_BEFORE s before the rim moment to WIN_AFTER s after, at FPS_OUT:
  - SAM3 video tracking, text prompt "basketball player": stable id, box, mask per frame
  - ball + hoop detector (the typing stage's own weights): EVERY candidate, not just the
    best one, so the logic can choose by trajectory instead of by confidence

Cache: <out>/cache/<clip>.npz  (masks stored at half SAM resolution, bit-packed)

Usage: perceive.py <clip.mp4> <out_dir> [rim_t=5.0]
"""
import os
import sys
import time

import cv2
import numpy as np
import torch

SAM3 = "/home/dev/sam3_model"
BALL_W = "/home/dev/shot_typing/yolo26s_ball_hoop_ft_evalweek_v1.pt"
WIN_BEFORE, WIN_AFTER = 4.0, 2.0
FPS_OUT = 15
SAM_W = 1024
MASK_DS = 2
DEV = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEV != "cpu" else torch.float32


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
    return full, np.array(times), fps


def sam3_players(full):
    from transformers import Sam3VideoModel, Sam3VideoProcessor
    proc = Sam3VideoProcessor.from_pretrained(SAM3)
    model = Sam3VideoModel.from_pretrained(SAM3, dtype=DTYPE).to(DEV).eval()
    H0, W0 = full[0].shape[:2]
    sc = SAM_W / float(W0)
    sh = int(round(H0 * sc))
    small = [cv2.cvtColor(cv2.resize(f, (SAM_W, sh)), cv2.COLOR_BGR2RGB) for f in full]
    sess = proc.init_video_session(video=small, inference_device=DEV, dtype=DTYPE)
    proc.add_text_prompt(sess, "basketball player")
    rows, masks = [], []            # rows: (frame, oid, score, x1, y1, x2, y2) full-res px
    with torch.inference_mode():
        for i in range(len(small)):
            mo = model(inference_session=sess, frame_idx=i)
            res = proc.postprocess_outputs(sess, mo, original_sizes=[[sh, SAM_W]])
            ids, bx, mk, scs = (res.get(k) for k in ("object_ids", "boxes", "masks", "scores"))
            if ids is None or bx is None or len(ids) == 0:
                continue
            ids = [int(v) for v in ids.tolist()]
            bx = bx.detach().to("cpu").float().numpy() / sc
            scs = scs.detach().to("cpu").float().numpy() if scs is not None else np.ones(len(ids))
            mk = mk.detach().to("cpu").numpy() > 0 if mk is not None else None
            for j, oid in enumerate(ids):
                rows.append((i, oid, float(scs[j]), *[float(v) for v in bx[j]]))
                m = mk[j] if mk is not None else np.zeros((sh, SAM_W), bool)
                m = cv2.resize(m.astype(np.uint8), (SAM_W // MASK_DS, sh // MASK_DS),
                               interpolation=cv2.INTER_NEAREST) > 0
                masks.append(np.packbits(m))
    del model, proc, sess
    torch.cuda.empty_cache()
    return np.array(rows, float).reshape(-1, 7), np.array(masks), (sh // MASK_DS, SAM_W // MASK_DS)


def detections(full):
    from ultralytics import YOLO
    m = YOLO(BALL_W)
    rows = []                         # (frame, cls, conf, x1, y1, x2, y2); cls 0 ball, 1 hoop
    for i in range(0, len(full), 8):
        for k, r in enumerate(m.predict(full[i:i + 8], imgsz=1280, conf=0.05, verbose=False, device=DEV)):
            for b in r.boxes:
                rows.append((i + k, int(b.cls.item()), float(b.conf.item()),
                             *[float(v) for v in b.xyxy[0]]))
    return np.array(rows, float).reshape(-1, 7)


def main():
    clip, out = sys.argv[1:3]
    rim_t = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0
    name = os.path.splitext(os.path.basename(clip))[0]
    os.makedirs(os.path.join(out, "cache"), exist_ok=True)
    t0 = time.time()
    full, times, fps = load_frames(clip, rim_t)
    players, masks, mshape = sam3_players(full)
    dets = detections(full)
    np.savez_compressed(os.path.join(out, "cache", name + ".npz"), times=times, rim_t=rim_t,
                        players=players, masks=masks, mask_shape=mshape, dets=dets,
                        frame_shape=full[0].shape[:2], sam_w=SAM_W, mask_ds=MASK_DS)
    print(name, "frames", len(full), "player rows", len(players), "dets", len(dets),
          "%.0fs" % (time.time() - t0), flush=True)


if __name__ == "__main__":
    main()

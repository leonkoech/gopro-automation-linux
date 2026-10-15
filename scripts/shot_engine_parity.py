"""Does the TensorRT FP16 engine call the same shots as the PyTorch model the live loop runs today?

usage: python3 scripts/shot_engine_parity.py <engine> <segment.mp4> [...]   (repo root, .env.agx sourced)
Per segment, exactly the live loop's scan (stride 4, imgsz 640) with each model, then logic.decide on the
canonical rim of that camera. Reports track agreement and every verdict that differs, plus scan times.
"""
import json
import os
import re
import sys
import time

sys.path.insert(0, os.getcwd())
from ultralytics import YOLO  # noqa: E402

from agx_pipeline.shot_detect import logic  # noqa: E402
from agx_pipeline.shot_detect.backtest import scan  # noqa: E402

PT = "agx_pipeline/shot_detect/weights/ball_yolo26s_gray_hifps_v3_best.pt"
RIMS = json.load(open("agx_pipeline/shot_detect/rims.json"))
FPS = 106.0
eng_path, segs = sys.argv[1], sys.argv[2:]
models = {"pt": YOLO(PT), "trt": YOLO(eng_path, task="detect")}
for m in models.values():                                     # warm both up
    scan.scan_ball_and_hoops(m, segs[0], 0, stride=4, imgsz=640)

times = {"pt": 0.0, "trt": 0.0}
n_pt = n_trt = n_both = 0
dxy = []
same_v = diff_v = total_v = 0
diffs = []
for p in segs:
    angle = re.search(r"_(SL|SR)\.mp4$", p).group(1)
    rim = RIMS.get(angle) if isinstance(RIMS, dict) else None
    out = {}
    for k, m in models.items():
        t = time.time()
        track, _ = scan.scan_ball_and_hoops(m, p, 0, stride=4, imgsz=640)
        times[k] += time.time() - t
        out[k] = track
    a = {r[0]: r for r in out["pt"]}
    b = {r[0]: r for r in out["trt"]}
    n_pt += len(a); n_trt += len(b)
    for i in set(a) & set(b):
        n_both += 1
        dxy.append(((a[i][1] - b[i][1]) ** 2 + (a[i][2] - b[i][2]) ** 2) ** 0.5)
    if rim is None:
        continue
    G = logic.Geo.from_rim(rim, FPS)
    va = [(round(v.get("t", 0), 1), v["verdict"]) for v in logic.decide(G, out["pt"]) if "verdict" in v]
    vb = [(round(v.get("t", 0), 1), v["verdict"]) for v in logic.decide(G, out["trt"]) if "verdict" in v]
    total_v += max(len(va), len(vb))
    if va == vb:
        same_v += len(va)
    else:
        diff_v += 1
        diffs.append((os.path.basename(p), va, vb))

dxy.sort()
print("segments", len(segs))
print("ball found: pt %d frames, trt %d frames, both %d (%.1f%% of pt)" % (n_pt, n_trt, n_both, 100.0 * n_both / max(1, n_pt)))
if dxy:
    print("position diff when both found: median %.2f px, p95 %.2f px, max %.1f px" % (
        dxy[len(dxy) // 2], dxy[int(0.95 * (len(dxy) - 1))], dxy[-1]))
print("shot verdicts: %d total, segments with identical verdicts: %d calls same, segments differing: %d" % (total_v, same_v, diff_v))
for d in diffs[:15]:
    print("  DIFF", d)
print("scan time per segment: pt %.2f s, trt %.2f s" % (times["pt"] / len(segs), times["trt"] / len(segs)))

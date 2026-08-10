"""Setup-2 (automated) core: scan a whole SL/SR video for shots, no triggers.

The user's design: a cheap coarse pass finds shot candidates, then the precise
120fps window is decided with the SAME v3 weights (no separate FL/FR model).

    scan_track()   -- stream the full video, run the v3 detector every `stride`
                      frames (the ~30fps spot pass), build one global ball track.
    estimate_rim() -- fit the rim ellipse from the detector's hoop (class-1) boxes
                      so each game self-calibrates its rim (cameras move between
                      games; don't trust a stale rims.json).
    logic.decide(Geo, track) then yields every rim-crossing verdict across the
    game -> the automated shot list. Precise per-shot confirmation at full fps is
    done by detect.ShotDetector.detect on the tight window (see run.confirm()).

Decode is streamed frame-by-frame off an ffmpeg pipe (never buffer the whole
video; a full SL clip is ~2.9GB / ~317k frames). ffmpeg `select` drops frames
before they cross the pipe, so the spot pass only pays to decode what it scans.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np

from agx_pipeline.shot_detect import logic
from agx_pipeline.shot_detect.detect import iter_frames  # streaming decode (shared)

BALL_CLASS = 0
HOOP_CLASS = 1


def _best_box(boxes, want_cls: int):
    """Highest-confidence (cx, cy, rb, conf) box of class `want_cls`, or None."""
    best = None
    for b in boxes:
        if int(b.cls.item()) != want_cls:
            continue
        x1, y1, x2, y2 = (float(v) for v in b.xyxy[0])
        cfd = float(b.conf.item())
        if best is None or cfd > best[3]:
            best = ((x1 + x2) / 2.0, (y1 + y2) / 2.0, max(x2 - x1, y2 - y1) / 2.0, cfd)
    return best


def estimate_rim(model, video_path: str, device: str, fps: float = 120.0,
                 sample_stride: int = 240, max_samples: int = 200,
                 conf: float = 0.20, imgsz: int = 1280, size=(720, 540)) -> Optional[Dict]:
    """Fit the rim ellipse from hoop (class-1) detections sampled across the game.

    Robust center/size = component-wise median of the hoop boxes; angle fixed ~90
    (the shot cams are level). Returns a rims.json-shaped dict, or None if the
    hoop is never seen (caller should fall back to canonical rims.json)."""
    cxs, cys, ws, hs = [], [], [], []
    for _, fr in iter_frames(video_path, size=size, select_stride=sample_stride):
        r = model.predict(fr, imgsz=imgsz, conf=conf, verbose=False, device=device)[0]
        hoop = _best_box(r.boxes, HOOP_CLASS)
        if hoop is not None:
            cxs.append(hoop[0]); cys.append(hoop[1])
        # also record raw box extents for semi-axes
        for b in r.boxes:
            if int(b.cls.item()) == HOOP_CLASS:
                x1, y1, x2, y2 = (float(v) for v in b.xyxy[0])
                ws.append(x2 - x1); hs.append(y2 - y1)
        if len(cxs) >= max_samples:
            break
    if not cxs:
        return None
    cx, cy = float(np.median(cxs)), float(np.median(cys))
    a = float(np.median(ws)) / 2.0 if ws else 100.0
    b = float(np.median(hs)) / 2.0 if hs else 100.0
    return {"center": [round(cx, 1), round(cy, 1)],
            "semi_axes": [round(a, 1), round(b, 1)], "angle": 90.0,
            "n_hoop_samples": len(cxs)}


def scan_track(model, video_path: str, device: str, stride: int = 4,
               conf: float = 0.20, imgsz: int = 1280, size=(720, 540),
               progress_every: int = 5000, limit_s: Optional[float] = None,
               batch: int = 16) -> Tuple[List[tuple], int]:
    """Full-video coarse ball track: the highest-conf class-0 box every `stride`
    frames -> (true_idx, x, y, rb, conf). stride=4 ≈ 30fps spot pass on 120fps
    footage. `limit_s` caps the scan to the first N seconds (bounded smoke).
    Inference is batched on the GPU (`batch`) — identical results, faster.
    Returns (track, n_frames_scanned)."""
    track: List[tuple] = []
    scanned = 0
    buf_idx: List[int] = []
    buf_fr: List[np.ndarray] = []

    def _flush():
        if not buf_fr:
            return
        results = model.predict(buf_fr, imgsz=imgsz, conf=conf, verbose=False, device=device)
        for j, r in enumerate(results):
            ball = _best_box(r.boxes, BALL_CLASS)
            if ball is not None:
                track.append((buf_idx[j], *ball))
        buf_idx.clear()
        buf_fr.clear()

    for idx, fr in iter_frames(video_path, size=size, select_stride=stride, t=limit_s):
        buf_idx.append(idx)
        buf_fr.append(fr)
        scanned += 1
        if len(buf_fr) >= batch:
            _flush()
        if progress_every and scanned % progress_every == 0:
            print(f"  scan {video_path.split('/')[-1]}: {scanned} sampled, "
                  f"{len(track)} ball frames, last_idx={idx}", flush=True)
    _flush()
    return track, scanned


def scan_shots(model, video_path: str, device: str, fps: float,
               rim: Dict, stride: int = 4, limit_s: Optional[float] = None,
               **kw) -> List[Dict]:
    """Coarse automated shot list for a whole video: scan -> logic.decide.

    Each returned dict is a crossing verdict with `t_shot` (seconds on the shot-cam
    clock) and `made`. These are approximate (coarse track); run.confirm() refines
    the ones we care about at full fps. `limit_s` caps the scan (bounded smoke)."""
    track, scanned = scan_track(model, video_path, device, stride=stride, limit_s=limit_s, **{
        k: v for k, v in kw.items() if k in {"conf", "imgsz", "size", "progress_every", "batch"}})
    G = logic.Geo.from_rim(rim, float(fps))
    shots = []
    for v in logic.decide(G, track):
        if "verdict" not in v:
            continue
        shots.append({
            "t_shot": round(float(v.get("t", 0.0)), 3),
            "frame": int(round(float(v.get("t", 0.0)) * fps)),
            "made": v["verdict"] == "MAKE",
            "verdict": v["verdict"],
            "rho": v.get("rho"),
            "decided_by": v.get("decided_by"),
        })
    shots.sort(key=lambda s: s["t_shot"])
    return shots

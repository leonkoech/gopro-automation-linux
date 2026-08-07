"""Windowed high-fps make/miss detector for the shot-detection trigger (Phase 1).

Given a short window of SL/SR frames around a scorekeeper trigger, run the v3
grayscale ball detector, build the ball track, and decide make/miss via the
aperture geometry in `logic.decide` (no color classifier — the validated
99-100% path). This is the windowed, self-contained port of
uball_shot_detection_dual_fusion_v2/near_v0/highfps/makemiss_v2.py: the
detector inference + ball-track build are lifted from its main loop
(highest-conf class-0 box per frame -> (idx, x, y, rb, conf)); the decision is
logic.decide (ported verbatim in this package).

Public API:
    det = ShotDetector(weight_path)               # loads YOLO v3 once
    v   = det.detect(frames, rim, fps=120)         # frames: list[BGR np.ndarray]
    # v: {"made", "verdict", "rho", "decided_by", "shot_frame", ...} or None

Frame extraction (sequential decode of a window from an mp4 — OpenCV seeks land
~13 frames early on this H.264, so NEVER seek) is provided by `read_window`.
"""
from __future__ import annotations

import os
from typing import List, Optional

import numpy as np

from agx_pipeline.shot_detect import logic

DET_CONF = float(os.environ.get("SHOT_DET_CONF", "0.20"))
DET_IMGSZ = int(os.environ.get("SHOT_DET_IMGSZ", "1280"))
BALL_CLASS = 0  # detector classes: 0 = Basketball, 1 = Basketball Hoop


class ShotDetector:
    """Loads the YOLO v3 grayscale ball detector once; reuse across triggers."""

    def __init__(self, weight_path: str, device: Optional[str] = None):
        import torch
        from ultralytics import YOLO
        self.device = device or (
            "cuda" if torch.cuda.is_available()
            else "mps" if torch.backends.mps.is_available() else "cpu")
        self.model = YOLO(weight_path)

    def _ball_track(self, frames) -> List[tuple]:
        """Per frame, the highest-conf Basketball box -> (idx, x, y, rb, conf).

        Mirrors makemiss_v2.py's per-frame selection: full-frame inference at
        imgsz=1280, class 0 only, keep the top-confidence box; rb = half the
        box's larger side. Frames with no ball are simply absent from the track.
        """
        track: List[tuple] = []
        for idx, fr in enumerate(frames):
            r = self.model.predict(fr, imgsz=DET_IMGSZ, conf=DET_CONF,
                                   verbose=False, device=self.device)[0]
            best = None
            for b in r.boxes:
                if int(b.cls.item()) != BALL_CLASS:
                    continue
                x1, y1, x2, y2 = (float(v) for v in b.xyxy[0])
                cfd = float(b.conf.item())
                if best is None or cfd > best[3]:
                    best = ((x1 + x2) / 2.0, (y1 + y2) / 2.0,
                            max(x2 - x1, y2 - y1) / 2.0, cfd)
            if best is not None:
                track.append((idx, *best))
        return track

    def detect(self, frames, rim, fps: float = 120.0,
               target_idx: Optional[int] = None) -> Optional[dict]:
        """Decide make/miss for a window. Returns None when no shot event.

        frames:     list of BGR np.ndarray (the window, one per frame, in order).
        rim:        {"center":[x,y], "semi_axes":[a,b], "angle":deg}.
        fps:        frame rate of the window (SHOT_FPS, ~120).
        target_idx: window frame nearest the trigger; picks the primary crossing
                    when several are found. Defaults to the window middle.
        """
        G = logic.Geo.from_rim(rim, float(fps))
        track = self._ball_track(frames)
        verdicts = [v for v in logic.decide(G, track) if "verdict" in v]
        if not verdicts:
            return None
        if target_idx is None:
            target_idx = len(frames) // 2
        primary = min(verdicts,
                      key=lambda d: abs(d.get("t", 0.0) * fps - target_idx))
        return {
            "made": primary["verdict"] == "MAKE",
            "verdict": primary["verdict"],
            "rho": primary.get("rho"),
            "decided_by": primary.get("decided_by"),
            "shot_frame": int(round(primary.get("t", 0.0) * fps)),
            "depth_in": primary.get("depth_in"),
            "lr_in": primary.get("lr_in"),
            "n_events": len(verdicts),
            "n_track": len(track),
            "all": verdicts,
        }


def read_window(video_path: str, frame_lo: int, frame_hi: int,
                fps: float = 120.0) -> List[np.ndarray]:
    """Sequentially decode frames [frame_lo, frame_hi] from an mp4 (fps ignored;
    kept for a uniform reader signature).

    NEVER uses CAP_PROP_POS_FRAMES — seeks land ~13 frames early on this 120fps
    H.264 (documented trap). Decodes forward from 0 and keeps the window. Correct
    but O(frame_hi) — for real-time use read_window_fast.
    """
    import cv2
    cap = cv2.VideoCapture(video_path)
    frames: List[np.ndarray] = []
    i = 0
    while i <= frame_hi:
        ok, fr = cap.read()
        if not ok:
            break
        if i >= frame_lo:
            frames.append(fr)
        i += 1
    cap.release()
    return frames


def read_window_fast(video_path: str, frame_lo: int, frame_hi: int,
                     fps: float = 120.0, margin_s: float = 1.0,
                     size=(720, 540)) -> List[np.ndarray]:
    """Decode ONLY the window via an ffmpeg keyframe seek — O(window), not
    O(frame_hi). ffmpeg `-ss` before `-i` seeks to the nearest keyframe <= t_lo
    then decodes forward, so returned frames span ~[frame_lo/fps - margin_s,
    frame_hi/fps + margin_s]; the exact start is keyframe-approximate (fine —
    the window has margin and make/miss is decided by ball geometry, not the
    boundary). This is the real-time path; the shot cams are a fixed 720x540.
    """
    import subprocess

    W, H = size
    stride = W * H * 3
    t_lo = max(0.0, frame_lo / fps - margin_s)
    dur = (frame_hi - frame_lo) / fps + 2.0 * margin_s
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error",
           "-ss", f"{t_lo:.3f}", "-i", video_path, "-t", f"{dur:.3f}",
           "-f", "rawvideo", "-pix_fmt", "bgr24", "-"]
    # Stream frame-by-frame off the pipe — never buffer the whole raw window in
    # RAM (a 10s window is ~1.4GB). Only the decoded frame list is held.
    frames: List[np.ndarray] = []
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            bufsize=stride)
    try:
        while True:
            buf = b""
            while len(buf) < stride:                 # a pipe read may be partial
                chunk = proc.stdout.read(stride - len(buf))
                if not chunk:
                    break
                buf += chunk
            if len(buf) < stride:
                break
            frames.append(np.frombuffer(buf, np.uint8).reshape(H, W, 3).copy())
    finally:
        if proc.stdout:
            proc.stdout.close()
        proc.wait()
    return frames

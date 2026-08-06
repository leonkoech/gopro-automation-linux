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


def read_window(video_path: str, frame_lo: int, frame_hi: int) -> List[np.ndarray]:
    """Sequentially decode frames [frame_lo, frame_hi] from an mp4.

    NEVER uses CAP_PROP_POS_FRAMES — seeks land ~13 frames early on this 120fps
    H.264 (documented trap). Decodes forward from 0 and keeps the window. For a
    live recording, frame_hi should be a frame already flushed to disk.
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

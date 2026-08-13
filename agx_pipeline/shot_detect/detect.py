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
DET_BATCH = int(os.environ.get("SHOT_DET_BATCH", "16"))  # GPU batch for inference
BALL_CLASS = 0  # detector classes: 0 = Basketball, 1 = Basketball Hoop


def _best_ball(boxes):
    """Highest-conf Basketball box -> (cx, cy, rb, conf), or None."""
    best = None
    for b in boxes:
        if int(b.cls.item()) != BALL_CLASS:
            continue
        x1, y1, x2, y2 = (float(v) for v in b.xyxy[0])
        cfd = float(b.conf.item())
        if best is None or cfd > best[3]:
            best = ((x1 + x2) / 2.0, (y1 + y2) / 2.0,
                    max(x2 - x1, y2 - y1) / 2.0, cfd)
    return best


class ShotDetector:
    """Loads the YOLO v3 grayscale ball detector once; reuse across triggers."""

    def __init__(self, weight_path: str, device: Optional[str] = None):
        import torch
        from ultralytics import YOLO
        # cuDNN autotune (benchmark=True, which ultralytics sets) re-probes
        # algorithms on the first inference of EVERY new batch shape — minutes
        # each on Jetson, and its workspace probing can OOM. We call predict with
        # varying batch sizes (window vs partial batch), so disable it: use the
        # default heuristic algorithm — fast + consistent first call, no re-tune.
        torch.backends.cudnn.benchmark = False
        self.device = device or (
            "cuda" if torch.cuda.is_available()
            else "mps" if torch.backends.mps.is_available() else "cpu")
        self.model = YOLO(weight_path)

    def _infer_batch(self, chunk, base_idx: int, track: List[tuple]) -> None:
        """Run one GPU batch and append any ball boxes to `track` (idx=base+j)."""
        results = self.model.predict(chunk, imgsz=DET_IMGSZ, conf=DET_CONF,
                                     verbose=False, device=self.device)
        for j, r in enumerate(results):
            ball = _best_ball(r.boxes)
            if ball is not None:
                track.append((base_idx + j, *ball))

    def _ball_track(self, frames) -> List[tuple]:
        """Ball track over a materialized frame list (batched GPU inference).

        Per-image detection is independent of the batch, so results are identical
        to per-frame calls, just faster. For large windows prefer _ball_track_stream
        (this holds the whole list in RAM)."""
        track: List[tuple] = []
        for start in range(0, len(frames), DET_BATCH):
            self._infer_batch(frames[start:start + DET_BATCH], start, track)
        return track

    def _ball_track_stream(self, frame_iter) -> tuple:
        """Ball track from an iterator of (local_idx, BGR frame) — never holds
        more than one batch in RAM (fixes the whole-window OOM). Returns
        (track, n_frames_seen)."""
        track: List[tuple] = []
        buf: List = []
        base = 0
        n = 0
        for _idx, fr in frame_iter:
            buf.append(fr)
            n += 1
            if len(buf) >= DET_BATCH:
                self._infer_batch(buf, base, track)
                base += len(buf)
                buf = []
        if buf:
            self._infer_batch(buf, base, track)
        return track, n

    def _decide(self, track: List[tuple], rim, fps: float,
                target_idx: Optional[int], n_frames: int) -> Optional[dict]:
        G = logic.Geo.from_rim(rim, float(fps))
        verdicts = [v for v in logic.decide(G, track) if "verdict" in v]
        if not verdicts:
            return None
        if target_idx is None:
            target_idx = n_frames // 2
        primary = min(verdicts, key=lambda d: abs(d.get("t", 0.0) * fps - target_idx))
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

    def detect(self, frames, rim, fps: float = 120.0,
               target_idx: Optional[int] = None) -> Optional[dict]:
        """Decide make/miss for a materialized window. Returns None when no shot
        event. frames: list of BGR np.ndarray in order. target_idx: window frame
        nearest the trigger (picks the primary crossing); defaults to the middle."""
        track = self._ball_track(frames)
        return self._decide(track, rim, fps, target_idx, len(frames))

    def detect_stream(self, frame_iter, rim, fps: float = 120.0,
                      target_idx: Optional[int] = None) -> Optional[dict]:
        """Streaming make/miss over an iterator of (idx, BGR frame) — memory is
        capped at one batch, so a full 120fps window never balloons RAM. Same
        verdict as detect() on the same frames."""
        track, n = self._ball_track_stream(frame_iter)
        return self._decide(track, rim, fps, target_idx, n)

    def empty_cache(self) -> None:
        """Release torch's CUDA cache (unified memory on Tegra = system RAM)."""
        try:
            import torch
            if str(self.device).startswith("cuda"):
                torch.cuda.empty_cache()
        except Exception:  # noqa: BLE001
            pass


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


def iter_frames(video_path: str, size=(720, 540), select_stride: int = 1,
                ss: Optional[float] = None, t: Optional[float] = None,
                max_frames: Optional[int] = None):
    """Yield (index, BGR frame) streamed from ffmpeg — never materializes the
    whole window (feed straight into ShotDetector.detect_stream). ss (seconds)
    seeks; select_stride>1 emits only every Nth frame (the yielded index is still
    the true frame number). The shot cams are a fixed 720x540 H.264.

    Window length is bounded by `max_frames` (a COUNT), NOT `-t` (seconds): the
    FLIR recordings carry a bogus timebase (r_frame_rate=12000) that makes
    ffmpeg's `-t` fail to stop, so a "10s window" would otherwise decode the whole
    file (-> detector over the whole game -> OOM). `-frames:v` + a hard Python cap
    are timebase-independent. `t` is kept only as a soft hint for well-formed files.
    """
    import subprocess

    W, H = size
    stride_bytes = W * H * 3
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error"]
    if ss is not None:
        cmd += ["-ss", f"{ss:.3f}"]
    cmd += ["-i", video_path]
    if select_stride > 1:
        cmd += ["-vf", f"select=not(mod(n\\,{select_stride}))"]
    # -vsync 0 (passthrough) ALWAYS: without it, ffmpeg's default frame-rate sync
    # drops/duplicates frames based on the FLIR files' bogus timebase, which over a
    # longer decode-after-seek silently corrupts frames so the ball vanishes (window
    # then finds an empty track). Passthrough emits each decoded frame as-is.
    cmd += ["-vsync", "0"]
    if max_frames is not None:
        cmd += ["-frames:v", str(int(max_frames))]   # robust: count, not duration
    elif t is not None:
        cmd += ["-t", f"{t:.3f}"]
    cmd += ["-f", "rawvideo", "-pix_fmt", "bgr24", "-"]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, bufsize=stride_bytes)
    rank = 0
    try:
        while True:
            if max_frames is not None and rank >= max_frames:
                break                                # hard cap (belt-and-suspenders)
            buf = b""
            while len(buf) < stride_bytes:
                chunk = proc.stdout.read(stride_bytes - len(buf))
                if not chunk:
                    break
                buf += chunk
            if len(buf) < stride_bytes:
                break
            yield rank * select_stride, np.frombuffer(buf, np.uint8).reshape(H, W, 3).copy()
            rank += 1
    finally:
        if proc.stdout:
            proc.stdout.close()
        try:
            proc.terminate()                         # stop ffmpeg if we broke early
        except Exception:  # noqa: BLE001
            pass
        proc.wait()

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

import json
import logging
import os
import threading
from typing import List, Optional

import numpy as np

from agx_pipeline.shot_detect import logic

DET_CONF = float(os.environ.get("SHOT_DET_CONF", "0.20"))
DET_IMGSZ = int(os.environ.get("SHOT_DET_IMGSZ", "1280"))
DET_BATCH = int(os.environ.get("SHOT_DET_BATCH", "16"))  # GPU batch for inference
ENGINE_MAX_BATCH = 16   # what scripts/build_shot_engine.py builds the TensorRT engine for
ENGINE_MAX_IMGSZ = 640  # ... and the largest image side it accepts
PROBE_TOL_PX = 3.0      # engine vs .pt on the probe ball/hoop (378 real segments: p95 0.14 px, max 3.5 px)
PROBE_BATCHES = (1, 3, ENGINE_MAX_BATCH)   # the live loop runs full and partial batches
PROBE_TIMEOUT_S = 90    # the child loads torch + the engine in ~9 s on the Orin
ENGINE_STRIKES = 3      # ordinary engine exceptions tolerated before it is switched off
# Written when the engine faults the GPU mid-run: the next service start then stays on the .pt
# until someone looks (delete the file to re-enable).
ENGINE_DISABLED_MARK = os.environ.get("SHOT_DET_ENGINE_DISABLED_MARK",
                                      "/home/dev/shot_det_engine.DISABLED")
_PROBE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "probe")
BALL_CLASS = 0  # detector classes: 0 = Basketball, 1 = Basketball Hoop
HOOP_CLASS = 1

_log = logging.getLogger("agx.shot_detect")


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


def _is_cuda_fault(e: Exception) -> bool:
    """A CUDA fault (illegal address and the like) poisons the whole process's CUDA context."""
    text = "%s %s" % (type(e).__name__, e)
    return any(k in text for k in ("CUDA error", "cudaError", "AcceleratorError", "illegal memory"))


class _EngineWithFallback:
    """The TensorRT model behind the detector's one predict() door, with the .pt beside it.

    - A call the engine was not built for (image side over ENGINE_MAX_IMGSZ, e.g. the shot rebuild at
      960 or a backtest at 1280, or more than ENGINE_MAX_BATCH frames) goes to the .pt.
    - Each model has its own lock: the live loop, ingest confirm and auto-detect share this detector,
      and an interleaved TensorRT context can return wrong boxes without raising. A slow 960 px
      rebuild on the .pt never blocks the live loop on the engine.
    - The .pt is loaded up front, so no call ever waits on a model load.
    - An ordinary engine exception is answered by the .pt; after ENGINE_STRIKES of them the engine is
      off for this process. A CUDA fault switches it off at once and writes ENGINE_DISABLED_MARK:
      the fault has poisoned this process's CUDA context, so the .pt may fail too until the service
      restarts, and the restart must not walk back into the same engine."""

    def __init__(self, engine, pt, owner):
        self._engine, self._pt, self._owner = engine, pt, owner
        self._engine_lock, self._pt_lock = threading.Lock(), threading.Lock()
        self._strikes, self._off = 0, False

    @staticmethod
    def _fits(source, imgsz) -> bool:
        side = max(imgsz) if isinstance(imgsz, (list, tuple)) else imgsz
        n = len(source) if isinstance(source, (list, tuple)) else 1
        return side is not None and int(side) <= ENGINE_MAX_IMGSZ and n <= ENGINE_MAX_BATCH

    def _switch_off(self, why: str, fault: bool) -> None:
        self._off = True
        self._owner.backend = "pytorch"
        if fault:
            try:
                with open(ENGINE_DISABLED_MARK, "w") as fh:
                    fh.write("%s\n" % why)
            except OSError:
                pass
            _log.critical("shot detector: TensorRT engine faulted the GPU mid-run (%s) — engine off, "
                          "%s written; this process's CUDA may be unusable until the service restarts",
                          why, ENGINE_DISABLED_MARK)
        else:
            _log.error("shot detector: TensorRT engine failed %d times (last: %s) — "
                       "switching to the .pt for the rest of this process", self._strikes, why)

    def predict(self, source, imgsz=None, **kwargs):
        if not self._off and self._fits(source, imgsz):
            with self._engine_lock:
                if not self._off:
                    try:
                        return self._engine.predict(source, imgsz=imgsz, **kwargs)
                    except Exception as e:  # noqa: BLE001 - answered by the .pt below
                        why = "%s: %s" % (type(e).__name__, str(e)[:200])
                        self._strikes += 1
                        if _is_cuda_fault(e):
                            self._switch_off(why, fault=True)
                        elif self._strikes >= ENGINE_STRIKES:
                            self._switch_off(why, fault=False)
                        else:
                            _log.warning("shot detector: TensorRT engine call failed (%d/%d): %s — "
                                         "this call answered by the .pt", self._strikes, ENGINE_STRIKES, why)
        with self._pt_lock:
            return self._pt.predict(source, imgsz=imgsz, **kwargs)


def _best_box(boxes, cls):
    best = None
    for b in boxes:
        if int(b.cls.item()) == cls and (best is None or float(b.conf.item()) > float(best.conf.item())):
            best = b
    if best is None:
        return None
    x1, y1, x2, y2 = (float(v) for v in best.xyxy[0])
    return (x1 + x2) / 2.0, (y1 + y2) / 2.0, x2 - x1, y2 - y1


def _probe_ok(model, device) -> Optional[str]:
    """None when the engine answers the fixture the way the .pt does, else why not.

    A wrong engine does not always raise: one built for 640x640 alone found 39 of 3512 ball frames
    (14 Oct 2026). The fixture is a real SL frame with the .pt's ball and hoop (probe/probe_ball.json).
    It is run at the size the live loop uses, in full and partial batches (the live loop runs both),
    and every answer must hold the ball (the track) and the hoop (the rim the make/miss is judged on)
    within PROBE_TOL_PX of the .pt's."""
    import cv2
    want = json.load(open(os.path.join(_PROBE_DIR, "probe_ball.json")))
    frame = cv2.imread(os.path.join(_PROBE_DIR, "probe_ball.png"))
    if frame is None:
        return "probe frame missing"
    for n in PROBE_BATCHES:
        results = model.predict([frame] * n, imgsz=ENGINE_MAX_IMGSZ, conf=DET_CONF,
                                verbose=False, device=device)
        if len(results) != n:
            return "batch %d: %d results" % (n, len(results))
        for r in results:
            ball, hoop = _best_box(r.boxes, BALL_CLASS), _best_box(r.boxes, HOOP_CLASS)
            if ball is None or hoop is None:
                return "batch %d: probe %s not found" % (n, "ball" if ball is None else "hoop")
            checks = (("ball", ball[0] - want["ball_cx"], ball[1] - want["ball_cy"]),
                      ("hoop", hoop[0] - want["hoop_cx"], hoop[1] - want["hoop_cy"]),
                      ("hoop size", hoop[2] - want["hoop_w"], hoop[3] - want["hoop_h"]))
            for what, dx, dy in checks:
                if (dx * dx + dy * dy) ** 0.5 > PROBE_TOL_PX:
                    return "batch %d: probe %s %.1f px from the .pt's" % (n, what, (dx * dx + dy * dy) ** 0.5)
    return None


def _probe_in_subprocess(path: str) -> Optional[str]:
    """Run _probe_ok on the engine in a throwaway process; None when it passes, else why not.

    Measured 15 Oct: a bad engine can fail with a CUDA illegal memory access, which poisons the CUDA
    context of the whole process -- the .pt then fails too, so an in-process probe would take the
    detector down instead of falling back. In a child process the fault dies with the child."""
    import signal
    import subprocess
    import sys
    code = ("import sys\n"
            "from ultralytics import YOLO\n"
            "from agx_pipeline.shot_detect.detect import _probe_ok\n"
            "why = _probe_ok(YOLO(sys.argv[1], task='detect'), 'cuda')\n"
            "print('PROBE_OK' if why is None else 'PROBE_FAIL ' + why, flush=True)\n")
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, [root, os.environ.get("PYTHONPATH")])))
    import tempfile
    with tempfile.TemporaryFile("w+") as out, tempfile.TemporaryFile("w+") as err:
        # Files, not pipes, and its own process group: a child wedged in the GPU driver is killed
        # with everything it started, and nothing here ever waits on it without a bound.
        proc = subprocess.Popen([sys.executable, "-c", code, path], stdout=out, stderr=err,
                                stdin=subprocess.DEVNULL, env=env, cwd=root, start_new_session=True)
        try:
            rc = proc.wait(timeout=PROBE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass                    # unkillable (stuck in the driver): give up on it, never block
            return "probe timed out after %ds" % PROBE_TIMEOUT_S
        out.seek(0)
        err.seek(0)
        stdout, stderr = out.read(), err.read()
    lines = [ln for ln in stdout.splitlines() if ln.startswith("PROBE_")]
    if lines and lines[-1] == "PROBE_OK":
        return None
    if lines:
        return lines[-1][len("PROBE_FAIL "):]
    err_lines = stderr.strip().splitlines() or ["no output"]
    last = next((ln for ln in reversed(err_lines) if "Error: " in ln),       # the exception line
                next((ln for ln in reversed(err_lines) if "Error" in ln), err_lines[-1]))
    return "probe process exited %d: %s" % (rc, last[:200])


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
        self.backend = "pytorch"
        engine = self._load_engine(YOLO)
        pt = YOLO(weight_path)
        self.model = _EngineWithFallback(engine, pt, self) if engine is not None else pt

    def _load_engine(self, YOLO):
        """The TensorRT build of the same detector (SHOT_DET_ENGINE), or None to use the .pt.

        On the Orin the engine takes inference from 1.39 s to 0.60 s per segment with the same calls
        (60 segments: 19 / 19 verdicts identical, ball positions within 0.3 px). An engine only runs
        on the device and TensorRT version that built it, and only for the shapes it was built for,
        so it is used only when it loads AND finds the probe frame's ball where the .pt does; anything
        else falls back to the .pt with the reason logged. Rebuild: scripts/build_shot_engine.py."""
        path = os.environ.get("SHOT_DET_ENGINE", "").strip()
        if not path:
            return None
        why = None
        if not os.path.isfile(path):
            why = "file not found"
        elif os.path.exists(ENGINE_DISABLED_MARK):
            why = "disabled after a GPU fault (%s exists)" % ENGINE_DISABLED_MARK
        elif not str(self.device).startswith("cuda"):
            why = "no CUDA device"
        if why is None:
            why = _probe_in_subprocess(path)
            if why is None:
                try:
                    model = YOLO(path, task="detect")
                    self.backend = "tensorrt"
                    _log.info("shot detector: TensorRT engine %s (probe ball found)", path)
                    return model
                except Exception as e:  # noqa: BLE001 - any load failure means: use the .pt
                    why = "%s: %s" % (type(e).__name__, str(e)[:200])
        _log.error("shot detector: TensorRT engine %s not used (%s) — running the .pt, slower", path, why)
        return None

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
    # SHOT_DECODE=nv routes decode to the Orin's dedicated NVDEC engine
    # (h264_nvv4l2dec in the Jetson ffmpeg build) — measured CPU decode of a 4s
    # 120fps segment is ~4x slower than real time, which the live loop can never
    # outrun with two angles. NVDEC shares nothing with the NVENC recorders.
    if os.environ.get("SHOT_DECODE", "").strip().lower() == "nv":
        cmd += ["-c:v", "h264_nvv4l2dec"]
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

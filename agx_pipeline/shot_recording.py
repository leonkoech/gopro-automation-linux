#!/usr/bin/env python3
"""
Shot-detection recording controller (FLIR Blackfly S / GigE Vision, Aravis).

The near-rim high-fps cameras are NOT RTSP/NDI like the Zowieteks — they're
GigE Vision machine-vision cameras driven by the `aravissrc` GStreamer element,
and their raw (Mono8) stream must be hardware-encoded on the AGX. The Aravis
GStreamer plugin lives on the AGX **host** (not inside the NDI Docker image),
so this recorder runs `gst-launch-1.0` directly on the host — one process per
camera (robust: one FLIR failing can't kill the other, and it's fully
decoupled from the RTSP recording of the game angles).

Clean stop uses SIGINT on a `gst-launch -1.0 -e` pipeline: `-e` sends EOS so
mp4mux finalizes the moov atom and the file is playable (verified on-box).

Exposes the SAME interface as RecordingController / CamrecController
(`start()`/`stop()`/`is_recording()` with identical return shapes), so
service.py orchestrates it alongside the tracking backend without special
cases; only the capture mechanism differs. Outputs carry `role`/`basket_side`
so ingestion routes shot footage to shot detection (never through the
tracking transcode/register path).

Pipeline (proven on-box, ~1080-equivalent load at 720x540):
    gst-launch-1.0 -e aravissrc camera-name=<ip|serial> do-timestamp=true
        exposure-auto=off exposure=<us> gain-auto=continuous
      ! video/x-raw,framerate=<fps>/1
      ! videoconvert ! nvvidconv
      ! nvv4l2h264enc bitrate=<bits> insert-sps-pps=1 iframeinterval=<n>
        idrinterval=<n> maxperf-enable=1
      ! h264parse ! mp4mux ! filesink location=<out>

Timing/rate design (CV-team findings, 2026-07-23 — measured on 2026-07-20 games):
- The caps filter locks a CONSTANT frame rate (aravissrc programs the camera's
  AcquisitionFrameRate from downstream caps). Free-running cameras drifted apart
  (SR ~161fps vs SL ~171fps on the same rig), which broke cross-camera sync.
  120fps is an integer multiple of the 30fps court video and leaves large
  bandwidth/exposure headroom, so the sensor can actually HOLD it.
- Auto-exposure silently caps the frame rate in a dim gym (the SR deficit, and
  the historical ~66fps night measurement). Exposure is therefore FIXED and
  short (default 3ms — also kills motion blur); brightness adapts via auto-GAIN,
  which never limits fps.
- `do-timestamp=true` stamps buffers with real capture time so dropped frames
  leave honest PTS gaps in the container instead of a uniformly re-stamped
  track that lies about recording time.
- Each start() writes a `<label>_shot_timing.json` sidecar with the wall-clock
  spawn time per camera, so cross-camera offset becomes a lookup, not an
  estimation. (A shared hardware trigger / per-frame FLIR hardware timestamps
  would be better still, but that is rig wiring, not software config.)
Ingestion still probes and records the ACTUAL delivered fps.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import time
from typing import Dict, List, Optional

from agx_pipeline.recording import Camera, Config, _probe, _utcnow

logger = logging.getLogger("agx.shot")

DEFAULT_BITRATE = os.getenv("SHOT_BITRATE", "8000000")            # NVENC bits/sec
DEFAULT_IFRAME = os.getenv("SHOT_IFRAME_INTERVAL", "30")
START_SETTLE_SEC = float(os.getenv("SHOT_START_SETTLE_SEC", "2.5"))
# Constant frame rate locked on the camera via caps (0 disables the lock and
# reverts to free-running — see module docstring for why locked 120 is the default).
SHOT_FPS = int(os.getenv("SHOT_FPS", "120"))
# Fixed exposure in µs, auto-exposure OFF (0 leaves the camera's own auto
# exposure untouched). Must stay well under the frame period (8333µs at 120fps).
SHOT_EXPOSURE_US = int(os.getenv("SHOT_EXPOSURE_US", "3000"))


class AravisRecorder:
    """Records the FLIR/GigE shot cameras via host `gst-launch`, one per camera."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        # label -> [ {angle, id, path, basket_side, proc} ]  (in-memory, this process)
        self._procs: Dict[str, List[Dict]] = {}

    # -- helpers ----------------------------------------------------------- #
    def _select(self, camera_ids: Optional[List[str]]) -> List[Camera]:
        if not camera_ids:
            return list(self.cfg.shot_cameras)
        return [c for cid in camera_ids
                for c in [self.cfg.shot_camera_by_id(cid)] if c]

    def _pipeline(self, cam: Camera, out_path: str) -> List[str]:
        src = ["aravissrc", f"camera-name={cam.camera_name or cam.ip}", "do-timestamp=true"]
        if SHOT_EXPOSURE_US > 0:
            src += ["exposure-auto=off", f"exposure={SHOT_EXPOSURE_US}", "gain-auto=continuous"]
        cmd = ["gst-launch-1.0", "-e", *src]
        if SHOT_FPS > 0:
            cmd += ["!", f"video/x-raw,framerate={SHOT_FPS}/1"]
        cmd += [
            "!", "videoconvert",
            "!", "nvvidconv",
            "!", "nvv4l2h264enc", f"bitrate={DEFAULT_BITRATE}",
            # idrinterval too: nvv4l2h264enc defaults idrinterval to 256, and only
            # IDR frames are seekable (same defect fixed in ingest._transcode_hw)
            "insert-sps-pps=1", f"iframeinterval={DEFAULT_IFRAME}",
            f"idrinterval={DEFAULT_IFRAME}", "maxperf-enable=1",
            "!", "h264parse", "!", "mp4mux",
            "!", "filesink", f"location={out_path}",
        ]
        return cmd

    def _outputs_for(self, procs: List[Dict], alive_only: bool) -> List[Dict]:
        out = []
        for pr in procs:
            if alive_only and pr["proc"].poll() is not None:
                continue
            out.append({"angle": pr["angle"], "id": pr["id"], "path": pr["path"],
                        "basket_side": pr["basket_side"], "role": "shot_detection"})
        return out

    # -- interface (matches RecordingController) --------------------------- #
    def is_recording(self, label: str) -> bool:
        procs = self._procs.get(label) or []
        return any(pr["proc"].poll() is None for pr in procs)

    def start(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        cams = self._select(camera_ids)
        if not cams:
            raise ValueError("no valid shot cameras selected")
        if self.is_recording(label):
            raise RuntimeError(f"shot session {label} already recording")
        session_dir = os.path.join(self.cfg.output_dir, label)
        os.makedirs(session_dir, exist_ok=True)
        procs: List[Dict] = []
        try:
            for cam in cams:
                out_path = os.path.join(session_dir, f"{label}_{cam.angle}.mp4")
                spawned_at = _utcnow()  # wall-clock anchor for cross-camera sync
                proc = subprocess.Popen(
                    self._pipeline(cam, out_path),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                procs.append({"angle": cam.angle, "id": cam.id, "path": out_path,
                              "basket_side": cam.basket_side, "proc": proc,
                              "spawned_at": spawned_at})
        except OSError:
            self._reap(procs)  # a mid-loop Popen failure must not leak earlier procs
            raise
        # let each gst-launch negotiate + start pulling frames, then keep only
        # the ones that stayed up (a missing/busy camera dies within this window)
        time.sleep(START_SETTLE_SEC)
        alive = [pr for pr in procs if pr["proc"].poll() is None]
        dead = [pr["angle"] for pr in procs if pr["proc"].poll() is not None]
        if dead:
            logger.warning("shot cameras did not start: %s", dead)
        if not alive:
            self._reap(procs)  # nothing came up — clean up any half-started procs
            raise RuntimeError("no shot cameras started (present? held by another process?)")
        self._procs[label] = procs
        self._write_timing_sidecar(session_dir, label, procs)
        outputs = self._outputs_for(procs, alive_only=True)
        logger.info("shot recording started label=%s angles=%s", label,
                    [o["angle"] for o in outputs])
        return {"label": label, "container": None, "outputs": outputs, "started_at": _utcnow()}

    def stop(self, label: str, outputs: Optional[List[Dict]] = None,
             finalize_timeout: int = 30) -> Dict[str, object]:
        procs = self._procs.pop(label, None)
        if procs:
            for pr in procs:
                if pr["proc"].poll() is None:
                    pr["proc"].send_signal(signal.SIGINT)  # -e -> EOS -> finalized moov
            deadline = time.monotonic() + finalize_timeout
            for pr in procs:
                self._wait_or_kill(pr["proc"], max(0.0, deadline - time.monotonic()))
        else:
            # No in-memory handles for this label (e.g. the CLI `stop` command).
            # Best effort — SIGINT any gst-launch still writing this session's
            # files. NOTE: the HTTP/relay path can't reach here after a service
            # restart — _do_stop returns 409 first once _current is empty — so
            # recovering a recording across a restart needs manual intervention.
            self._pkill_session(label)
        outs = outputs or (self._outputs_for(procs, alive_only=False) if procs else [])
        results = [{**o, **_probe(o["path"])} for o in outs]
        logger.info("shot stopped label=%s finalized=%s", label,
                    [r["angle"] for r in results if r.get("ok")])
        return {"label": label, "stopped_at": _utcnow(), "files": results}

    # -- internals --------------------------------------------------------- #
    @staticmethod
    def _write_timing_sidecar(session_dir: str, label: str, procs: List[Dict]) -> None:
        """Persist per-camera wall-clock spawn times so downstream sync is a
        lookup (frame N ≈ spawned_at + pipeline latency + N/fps) instead of an
        estimation. Best-effort: a sidecar failure must never kill a recording."""
        sidecar = {
            "label": label,
            "written_at": _utcnow(),
            "fps_lock": SHOT_FPS or None,
            "exposure_us": SHOT_EXPOSURE_US or None,
            "cameras": [{"angle": pr["angle"], "id": pr["id"], "path": pr["path"],
                         "spawned_at": pr["spawned_at"],
                         "alive_after_settle": pr["proc"].poll() is None}
                        for pr in procs],
        }
        path = os.path.join(session_dir, f"{label}_shot_timing.json")
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(sidecar, fh, indent=2)
        except OSError as e:
            logger.warning("could not write shot timing sidecar %s: %s", path, e)

    @staticmethod
    def _wait_or_kill(proc: subprocess.Popen, timeout: float) -> None:
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    @staticmethod
    def _reap(procs: List[Dict]) -> None:
        for pr in procs:
            p = pr["proc"]
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()

    def _pkill_session(self, label: str) -> None:
        session_dir = os.path.join(self.cfg.output_dir, label)
        try:
            # match the filesink location for this session so we only hit our procs
            subprocess.run(["pkill", "--signal", "INT", "-f", f"filesink location={session_dir}"],
                           stdin=subprocess.DEVNULL)
            time.sleep(3)
        except Exception as e:  # noqa: BLE001
            logger.warning("shot pkill fallback failed for %s: %s", label, e)


# --------------------------------------------------------------------------- #
# CLI (for on-box testing)
# --------------------------------------------------------------------------- #
def main() -> int:
    import argparse
    import json

    from agx_pipeline.recording import load_config

    ap = argparse.ArgumentParser(description="AGX shot (FLIR/Aravis) recorder")
    ap.add_argument("action", choices=["start", "stop", "status", "dry-run"])
    ap.add_argument("--label", required=True)
    ap.add_argument("--cameras", help="comma-separated shot camera ids (default: all)")
    ap.add_argument("--seconds", type=int, default=8, help="start action: record N seconds then stop")
    args = ap.parse_args()

    cfg = load_config()
    rec = AravisRecorder(cfg)
    cam_ids = args.cameras.split(",") if args.cameras else None

    if args.action == "dry-run":
        for cam in rec._select(cam_ids):
            out = os.path.join(cfg.output_dir, args.label, f"{args.label}_{cam.angle}.mp4")
            print(f"{cam.angle} ({cam.id}):", " ".join(rec._pipeline(cam, out)))
    elif args.action == "start":
        started = rec.start(args.label, cam_ids)
        print(json.dumps(started, indent=2))
        time.sleep(args.seconds)
        print(json.dumps(rec.stop(args.label, outputs=started["outputs"]), indent=2))
    elif args.action == "stop":
        print(json.dumps(rec.stop(args.label), indent=2))
    elif args.action == "status":
        print(json.dumps({"label": args.label, "recording": rec.is_recording(args.label)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

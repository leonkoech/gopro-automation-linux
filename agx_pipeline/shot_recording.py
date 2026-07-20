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
    gst-launch-1.0 -e aravissrc camera-name=<ip|serial>
      ! videoconvert ! nvvidconv
      ! nvv4l2h264enc bitrate=<bits> insert-sps-pps=1 iframeinterval=<n> maxperf-enable=1
      ! h264parse ! mp4mux ! filesink location=<out>

NOTE: effective fps is exposure-limited on the camera (ExposureAuto=Continuous
in a dim gym caps ~66fps). That's a camera-config/lighting matter, not a
pipeline one — this recorder captures whatever the sensor delivers; ingestion
probes and records the ACTUAL fps.
"""

from __future__ import annotations

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
        return [
            "gst-launch-1.0", "-e",
            "aravissrc", f"camera-name={cam.camera_name or cam.ip}",
            "!", "videoconvert",
            "!", "nvvidconv",
            "!", "nvv4l2h264enc", f"bitrate={DEFAULT_BITRATE}",
            "insert-sps-pps=1", f"iframeinterval={DEFAULT_IFRAME}", "maxperf-enable=1",
            "!", "h264parse", "!", "mp4mux",
            "!", "filesink", f"location={out_path}",
        ]

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
        for cam in cams:
            out_path = os.path.join(session_dir, f"{label}_{cam.angle}.mp4")
            proc = subprocess.Popen(
                self._pipeline(cam, out_path),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            procs.append({"angle": cam.angle, "id": cam.id, "path": out_path,
                          "basket_side": cam.basket_side, "proc": proc})
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
            # service restarted between start and stop: no in-memory handles. Best
            # effort — signal any gst-launch still writing this session's files.
            self._pkill_session(label, outputs)
        outs = outputs or (self._outputs_for(procs, alive_only=False) if procs else [])
        results = [{**o, **_probe(o["path"])} for o in outs]
        logger.info("shot stopped label=%s finalized=%s", label,
                    [r["angle"] for r in results if r.get("ok")])
        return {"label": label, "stopped_at": _utcnow(), "files": results}

    # -- internals --------------------------------------------------------- #
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

    def _pkill_session(self, label: str, outputs: Optional[List[Dict]]) -> None:
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

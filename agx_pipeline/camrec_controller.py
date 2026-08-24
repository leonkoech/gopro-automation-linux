#!/usr/bin/env python3
"""
Recording backend that drives geoffbauer's `camrec` FastAPI service instead of
our own single `gst-launch`.

Why: camrec records each camera as an **independent** ffmpeg process with a
per-camera watchdog (auto-restart on death/stall). That removes our biggest
weakness — a single `gst-launch` dies if any one `rtspsrc` fails, so one down
or mid-game-dropped camera kills the whole recording. camrec keeps the healthy
cameras rolling and self-heals the flapping ones.

This adapter exposes the SAME interface as `RecordingController`
(`start()`/`stop()`/`is_recording()` with identical return shapes), so
`service.py`, `sessions.py` and `ingest.py` are unchanged — only *how the
pixels get recorded* swaps. Select it with `RECORDING_BACKEND=camrec`.

Coordination requirements (see docs / the note to geoffbauer):
- camrec runs on the same AGX; we call it at `CAMREC_URL` (default
  http://localhost:8000) — localhost, so no tunnel/auth needed.
- camrec's RECORD_DIR (host side) must be `CAMREC_RECORD_DIR`, and that dir
  MUST live under the AGX `app_mount` (/home/dev/app) so the ingest transcode
  container (which mounts app_mount:/app/data) can read the recorded files.
- Segmenting is left OFF (we send no `segment_s`) for the locked
  "1 video / angle / game" model — one file per camera per game.
"""

from __future__ import annotations

import glob
import logging
import os
import time
from typing import Dict, List, Optional

import requests

from agx_pipeline.recording import Config, _probe, _utcnow

logger = logging.getLogger("agx.camrec")

STREAM_TYPE = os.getenv("CAMREC_STREAM_TYPE", "rtsp")  # rtsp (ffmpeg) | ndi (gstreamer)


class CamrecController:
    """Drop-in replacement for RecordingController backed by the camrec API."""

    def __init__(self, cfg: Config, base_url: Optional[str] = None,
                 record_dir: Optional[str] = None):
        self.cfg = cfg
        self.base_url = (base_url or os.getenv("CAMREC_URL", "http://localhost:8000")).rstrip("/")
        # Host-side dir where camrec writes; must be under app_mount for the
        # ingest transcode container to read it.
        self.record_dir = record_dir or os.getenv(
            "CAMREC_RECORD_DIR", os.path.join(cfg.app_mount, "camrec"))
        self.timeout = float(os.getenv("CAMREC_HTTP_TIMEOUT", "15"))

    # -- helpers ----------------------------------------------------------- #
    def _select(self, camera_ids: Optional[List[str]]):
        if not camera_ids:
            return list(self.cfg.cameras)
        return [c for cid in camera_ids for c in [self.cfg.camera_by_id(cid)] if c]

    def _expected_path(self, cam_id: str, label: str) -> str:
        """camrec single-run name: camera<ID>_<label>.mp4 (segmenting off)."""
        return os.path.join(self.record_dir, f"camera{cam_id}_{label}.mp4")

    def _resolve_file(self, cam_id: str, label: str) -> Optional[str]:
        """Find the actual recorded file, tolerating watchdog restarts (_r2 …)
        and any stray segment suffix. Picks the largest match; warns if the
        watchdog produced more than one run (concatenation is a known TODO)."""
        matches = glob.glob(os.path.join(self.record_dir, f"camera{cam_id}_{label}*.mp4"))
        matches = [m for m in matches if os.path.isfile(m)]
        if not matches:
            return None
        if len(matches) > 1:
            logger.warning("camrec: camera %s produced %d runs for %s (watchdog restart?) — "
                           "using largest; segments are not concatenated yet: %s",
                           cam_id, len(matches), label, [os.path.basename(m) for m in matches])
        return max(matches, key=lambda p: os.path.getsize(p))

    def _status(self) -> Dict:
        r = requests.get(f"{self.base_url}/api/status", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # -- interface (matches RecordingController) --------------------------- #
    def is_recording(self, label: str) -> bool:
        try:
            st = self._status()
        except Exception:  # noqa: BLE001
            return False
        return any(c.get("busy") and (c.get("recorder") or {}).get("label") == label
                   for c in st.get("cameras", []))

    def start(self, label: str, camera_ids: Optional[List[str]] = None) -> Dict[str, object]:
        cams = self._select(camera_ids)
        if not cams:
            raise ValueError("no valid cameras selected")
        started: List[str] = []
        for c in cams:
            try:
                r = requests.post(f"{self.base_url}/api/cameras/{c.id}/start",
                                  json={"stream_type": STREAM_TYPE, "label": label},
                                  timeout=self.timeout)
                if r.ok or r.status_code == 409:  # 409 = already recording -> a file will exist
                    started.append(c.id)
                else:
                    logger.warning("camrec: start camera %s (%s) failed: %s %s",
                                   c.id, c.angle, r.status_code, r.text[:200])
            except Exception as e:  # noqa: BLE001
                logger.warning("camrec: start camera %s (%s) error: %s", c.id, c.angle, e)
        if not started:
            raise RuntimeError("camrec: no cameras started (is the camrec service up "
                               f"at {self.base_url}? are the cameras streaming?)")
        down = [c.angle for c in cams if c.id not in started]
        if down:
            logger.warning("camrec: cameras not started (watchdog will keep retrying): %s", down)
        # outputs cover every requested camera; angles that never produce a file
        # are dropped at stop() via _probe(ok=False), same as the gst backend.
        outputs = [{"angle": c.angle, "id": c.id, "path": self._expected_path(c.id, label)}
                   for c in cams]
        logger.info("camrec: recording started label=%s angles=%s", label,
                    [c.angle for c in cams if c.id in started])
        return {"label": label, "container": None, "outputs": outputs, "started_at": _utcnow()}

    def stop(self, label: str, outputs: Optional[List[Dict]] = None,
             finalize_timeout: int = 30) -> Dict[str, object]:
        try:
            requests.post(f"{self.base_url}/api/cameras/stop_all", timeout=self.timeout)
        except Exception as e:  # noqa: BLE001
            logger.warning("camrec: stop_all error: %s", e)
        # wait for the recorders to finalize (SIGINT -> moov -> playable file)
        for _ in range(finalize_timeout):
            try:
                if not any(c.get("busy") for c in self._status().get("cameras", [])):
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(1)
        outs = outputs or [{"angle": c.angle, "id": c.id,
                            "path": self._expected_path(c.id, label)} for c in self.cfg.cameras]
        results = []
        for o in outs:
            path = self._resolve_file(o["id"], label) or o["path"]
            results.append({**o, "path": path, **_probe(path)})
        finalized = [r["angle"] for r in results if r.get("ok")]
        logger.info("camrec: stopped label=%s finalized=%s", label, finalized)
        return {"label": label, "stopped_at": _utcnow(), "files": results}

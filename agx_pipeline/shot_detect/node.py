"""Env-gated shot-detection validation node (Phase 1, shadow).

**OFF by default.** Set `SHOT_VALIDATION_ENABLED=true` (in `.env.agx`) to turn it
on — and only after the ML runtime (torch/ultralytics/opencv) + the v3 weight are
deployed on the box and the node has been tested. When on, each scorekeeper score
trigger is validated against the high-fps SL/SR footage on a daemon thread and the
verdict is written to Firebase (`highlights.{logId}.validation` + a game tally) for
the TV. Best-effort throughout: it must never block or break the highlight clip or
the relay, and a missing runtime / sidecar / footage just skips silently.

Env:
    SHOT_VALIDATION_ENABLED    "true"/"false"  (default false)
    SHOT_VALIDATION_N_BEFORE_S  reaction look-back seconds (default 8)
    SHOT_VALIDATION_M_AFTER_S   forward margin seconds     (default 2)
    SHOT_VALIDATION_LATENCY_S   sidecar spawn->first-frame latency (default 0)
    SHOT_DET_WEIGHT             path to ball_yolo26s_gray_hifps_v3_best.pt
"""
from __future__ import annotations

import json
import os
import threading
from typing import Optional

from logging_service import get_logger

logger = get_logger("agx.shot_validation")

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_WEIGHT = os.path.join(_HERE, "weights", "ball_yolo26s_gray_hifps_v3_best.pt")


def enabled() -> bool:
    return os.getenv("SHOT_VALIDATION_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def _f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


class _Validator:
    """Holds the detector + rims, loaded lazily the first time the node fires."""

    def __init__(self):
        self._detector = None
        self._rims = None
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            if self._detector is None:
                from agx_pipeline.shot_detect.detect import ShotDetector
                weight = os.getenv("SHOT_DET_WEIGHT", _DEFAULT_WEIGHT)
                self._detector = ShotDetector(weight)
                with open(os.path.join(_HERE, "rims.json")) as fh:
                    self._rims = json.load(fh)
                logger.info("shot-validation: detector loaded (%s)", weight)
            return self._detector, self._rims


_VALIDATOR = _Validator()


def validate_async(fb, cfg, cmd: dict, game_id: str, label: Optional[str],
                   starting_side_team1: Optional[str]) -> None:
    """Fire-and-forget validation for one score trigger. No-op unless enabled."""
    if not enabled() or not label:
        return
    threading.Thread(target=_run,
                     args=(fb, cfg, cmd, game_id, label, starting_side_team1),
                     name="shot-validate", daemon=True).start()


def _run(fb, cfg, cmd, game_id, label, starting_side_team1) -> None:
    try:
        from agx_pipeline.shot_detect.detect import read_window_fast
        from agx_pipeline.shot_detect.validate import validate_shot, write_validation
        detector, rims = _VALIDATOR.get()
    except Exception as e:  # noqa: BLE001 — runtime missing => node stays dark
        logger.warning("shot-validation runtime unavailable (%s) — skipping", e)
        return
    try:
        session_dir = os.path.join(cfg.output_dir, label)
        sidecar_path = os.path.join(session_dir, f"{label}_shot_timing.json")
        if not os.path.exists(sidecar_path):
            logger.info("shot-validation: no sidecar %s — skip", sidecar_path)
            return
        with open(sidecar_path) as fh:
            sidecar = json.load(fh)

        def video_for(angle: str) -> str:
            return os.path.join(session_dir, f"{label}_{angle}.mp4")

        trigger = {"ts": cmd.get("ts"), "team": cmd.get("team"),
                   "period": cmd.get("period")}
        val = validate_shot(
            detector, sidecar, video_for, trigger, starting_side_team1, rims,
            n_before_s=_f("SHOT_VALIDATION_N_BEFORE_S", 8.0),
            m_after_s=_f("SHOT_VALIDATION_M_AFTER_S", 2.0),
            pipeline_latency_s=_f("SHOT_VALIDATION_LATENCY_S", 0.0),
            reader=read_window_fast)
        log_id = str(cmd.get("logId") or cmd.get("log_id") or "")
        if val:
            write_validation(fb, game_id, log_id, val)
            logger.info("shot-validation %s: cam=%s cv_made=%s agrees=%s (%dms)",
                        log_id, val["cam"], val["cv_made"], val["agrees"],
                        val["latency_ms"])
        else:
            logger.info("shot-validation %s: no verdict (side/anchor/footage)", log_id)
    except Exception as e:  # noqa: BLE001 — shadow node must never raise
        logger.warning("shot-validation failed: %s", e)

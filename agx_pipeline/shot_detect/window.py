"""Map a scorekeeper trigger (wall-clock) to a frame window on a shot cam,
using the recorder's timing sidecar `{label}_shot_timing.json` (written by
agx_pipeline.shot_recording: per-camera `spawned_at` + `fps_lock`).

Frame of the trigger:  N ≈ (T_trigger − spawned_at − pipeline_latency) × fps
The window looks back `n_before_s` (operator reaction lag after the make) and
forward `m_after_s`. Pure Python — no ML / video deps.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional


def _iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def camera_entry(sidecar: dict, angle: str) -> Optional[dict]:
    for c in sidecar.get("cameras", []) or []:
        if c.get("angle") == angle:
            return c
    return None


def frame_window(sidecar: dict, angle: str, trigger_iso: str,
                 n_before_s: float = 8.0, m_after_s: float = 2.0,
                 pipeline_latency_s: float = 0.0) -> Optional[dict]:
    """Return {frame_lo, frame_hi, trigger_frame, fps, spawned_at} for `angle`
    (SL/SR), or None if the sidecar has no usable anchor for that camera.
    """
    cam = camera_entry(sidecar, angle)
    if not cam or not cam.get("spawned_at"):
        return None
    fps = float(sidecar.get("fps_lock") or 120.0)
    try:
        dt = (_iso(trigger_iso) - _iso(cam["spawned_at"])).total_seconds() - pipeline_latency_s
    except (ValueError, TypeError):
        return None
    if dt < 0:
        return None  # trigger before the recording started — nothing to look at
    trigger_frame = int(round(dt * fps))
    frame_lo = max(0, int(round((dt - n_before_s) * fps)))
    frame_hi = int(round((dt + m_after_s) * fps))
    return {"frame_lo": frame_lo, "frame_hi": frame_hi,
            "trigger_frame": trigger_frame, "fps": fps,
            "spawned_at": cam["spawned_at"]}

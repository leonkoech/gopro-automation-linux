"""Phase-1 shadow validation: given a scorekeeper score trigger, check the
high-fps shot cam for a made shot around that moment and return a verdict.

Chain: trigger (team, period, ts) -> scoring hoop side -> SL/SR -> sidecar frame
window -> decode -> ShotDetector.detect -> verdict. A *score* trigger expects a
MADE shot, so agreement = a MAKE crossing exists in the window. Shadow only:
never changes cards; the caller writes the verdict to Firebase for the TV.
"""
from __future__ import annotations

import time
from typing import Callable, Optional

from agx_pipeline.shot_detect.window import frame_window

# left hoop is filmed by SL, right hoop by SR (near-rim shot cams).
_HOOP_CAM = {"left": "SL", "right": "SR"}


def shot_cam_for(team: Optional[str], period: Optional[str],
                 starting_side_team1: Optional[str]) -> tuple:
    """(cam, hoop_side) for a score by `team`, or (None, None) if side unknown."""
    from agx_pipeline.side_attribution import scoring_hoop_side
    side = scoring_hoop_side(team, period, starting_side_team1)
    return _HOOP_CAM.get(side), side


def validate_shot(detector, sidecar: dict, video_for: Callable[[str], str],
                  trigger: dict, starting_side_team1: Optional[str],
                  rims: dict, n_before_s: float = 8.0, m_after_s: float = 2.0,
                  pipeline_latency_s: float = 0.0) -> Optional[dict]:
    """Validate one score trigger against the high-fps footage.

    detector:  a ShotDetector (lazily holds torch/ultralytics).
    sidecar:   parsed {label}_shot_timing.json (fps_lock + cameras[]).
    video_for: angle ("SL"/"SR") -> path to that cam's mp4 for this game.
    trigger:   {"ts": iso, "team": "left"|"right", "period": str}.
    rims:      {"SL": {...}, "SR": {...}} rim ellipses.
    Returns a validation dict, or None when the side/anchor/footage is missing.
    """
    from agx_pipeline.shot_detect.detect import read_window
    t0 = time.time()
    cam, side = shot_cam_for(trigger.get("team"), trigger.get("period"),
                             starting_side_team1)
    if cam is None:
        return None
    win = frame_window(sidecar, cam, trigger.get("ts"), n_before_s, m_after_s,
                       pipeline_latency_s)
    if win is None:
        return None
    frames = read_window(video_for(cam), win["frame_lo"], win["frame_hi"])
    if not frames:
        return None
    rim = (rims or {}).get(cam)
    if rim is None:
        return None
    target = win["trigger_frame"] - win["frame_lo"]
    v = detector.detect(frames, rim, fps=win["fps"], target_idx=target)
    crossings = (v or {}).get("all") or []
    n_make = sum(1 for c in crossings if c.get("verdict") == "MAKE")
    n_miss = sum(1 for c in crossings if c.get("verdict") == "MISS")
    cv_made = n_make > 0
    primary = None
    if v:
        primary = {k: v.get(k) for k in ("verdict", "rho", "decided_by", "shot_frame")}
    return {
        "cam": cam, "side": side,
        "cv_made": cv_made,
        "agrees": cv_made,               # score trigger => a made shot is expected
        "primary": primary,
        "n_make": n_make, "n_miss": n_miss, "n_events": len(crossings),
        "window_s": [n_before_s, m_after_s],
        "latency_ms": int((time.time() - t0) * 1000),
    }


def write_validation(fb, game_id: str, log_id: str, val: dict) -> None:
    """Best-effort: store the verdict on the highlight + bump the game tally.
    Shadow only — never touches plays/cards. Used by the box wiring (P1.2)."""
    try:
        ref = fb.db.collection("basketball-games").document(game_id)
        ref.update({f"highlights.{log_id}.validation": val})
        from google.cloud import firestore as _fs  # type: ignore
        ref.update({
            "highlights_validation.total": _fs.Increment(1),
            "highlights_validation.correct": _fs.Increment(1 if val.get("agrees") else 0),
        })
    except Exception:  # noqa: BLE001 — validation must never break ingestion/relay
        pass

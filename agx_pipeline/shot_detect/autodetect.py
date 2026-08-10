"""Phase 2 — AUTOMATED shot detection (no scorekeeper trigger).

Where the QA layer (Phase 1) validates the makes a human *marked*, this scans the
SL/SR high-fps footage end-to-end and finds EVERY shot on its own — the ball track
+ rim-crossing geometry decide make/miss with nobody marking anything. Each detected
shot is mapped back to the game timeline via the timing sidecar (spawned_at) and
written to `game.shot_auto`.

Runs inside the SAME deferred, GPU-gated, abortable worker as the QA (only when the
GPU is free; yields the instant a game starts recording). Gated by SHOT_AUTO_ENABLED
(default OFF) — ships dormant, rolled out after the manual QA is proven live.

Validated in the backtest: ~80% of shots detected automatically, ~96% make/miss on
the detected ones. This is the roadmap beyond scorekeeper-triggered QA (hands-off
stats when there is no scorekeeper).
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_auto")

_HOOP_SIDE = {"SL": "left", "SR": "right"}   # near-rim cam -> which hoop it films


def auto_enabled() -> bool:
    return os.getenv("SHOT_AUTO_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def _spawned_at(sidecar: dict, cam: str) -> Optional[str]:
    for c in (sidecar or {}).get("cameras", []):
        if c.get("angle") == cam:
            return c.get("spawned_at")
    return None


def _wallclock(spawned_iso: Optional[str], t_shot: float) -> Optional[str]:
    """SL/SR clock (seconds into that cam's video) -> game-timeline wall-clock."""
    try:
        return (datetime.fromisoformat(spawned_iso) +
                timedelta(seconds=float(t_shot))).isoformat()
    except Exception:  # noqa: BLE001
        return None


def run_autodetect(fb, cfg, game_id: str, sidecar: dict,
                   video_for: Callable[[str], str], starting_side: Optional[str] = None,
                   keep_going: Optional[Callable[[], bool]] = None) -> Optional[dict]:
    """Auto-detect every shot on SL+SR for one game. Returns the summary, or
    {'aborted': True} if a game started recording mid-scan. Shadow only."""
    if not auto_enabled():
        return None
    try:
        from agx_pipeline.shot_detect.backtest import scan
        from agx_pipeline.shot_detect.node import _VALIDATOR
    except Exception as e:  # noqa: BLE001 — runtime missing => stay dark
        logger.warning("shot-auto runtime unavailable (%s) — skipping", e)
        return None
    try:
        detector, rims = _VALIDATOR.get()
        fps = float((sidecar or {}).get("fps_lock") or 119.9)
        imgsz = int(os.getenv("SHOT_DET_IMGSZ", "640"))
        stride = int(os.getenv("SHOT_AUTO_STRIDE", "4"))     # ~30fps spot pass
        t0 = time.time()
        shots: List[dict] = []
        for cam in ("SL", "SR"):
            if keep_going is not None and not keep_going():
                return {"aborted": True}
            video = video_for(cam)
            if not os.path.exists(video):
                continue
            rim = (scan.estimate_rim(detector.model, video, detector.device,
                                     fps=fps, imgsz=imgsz)
                   or (rims or {}).get(cam))
            if rim is None:
                continue
            found = scan.scan_shots(detector.model, video, detector.device, fps, rim,
                                    stride=stride, imgsz=imgsz, progress_every=0,
                                    keep_going=keep_going)
            if keep_going is not None and not keep_going():
                return {"aborted": True}     # aborted mid-scan -> re-queue
            spawned = _spawned_at(sidecar, cam)
            for s in found:
                shots.append({
                    "cam": cam, "side": _HOOP_SIDE.get(cam),
                    "t_shot": s["t_shot"], "made": s["made"], "verdict": s["verdict"],
                    "rho": s.get("rho"), "wallclock": _wallclock(spawned, s["t_shot"]),
                })
        shots.sort(key=lambda s: (s.get("wallclock") or ""))
        n_make = sum(1 for s in shots if s["made"])
        summary = {
            "n_shots": len(shots), "n_make": n_make, "n_miss": len(shots) - n_make,
            "n_sl": sum(1 for s in shots if s["cam"] == "SL"),
            "n_sr": sum(1 for s in shots if s["cam"] == "SR"),
            "shots": shots[:400], "imgsz": imgsz, "stride": stride,
            "secs": round(time.time() - t0, 1), "aborted": False,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            fb.db.collection("basketball-games").document(game_id).update(
                {"shot_auto": summary})
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-auto write failed: %s", e)
        logger.info("shot-auto %s: %d shots (%d make / %d miss) in %.0fs",
                    game_id, len(shots), n_make, len(shots) - n_make, summary["secs"])
        return summary
    except Exception as e:  # noqa: BLE001 — Phase-2 must never break the worker
        logger.warning("shot-auto failed for %s: %s", game_id, e)
        return None

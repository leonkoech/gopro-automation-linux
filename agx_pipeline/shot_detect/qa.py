"""Post-game shot-detection QA (batch) — validate the scorekeeper's scored makes.

Runs as an ingest stage (STAGE 4.5) after the SL/SR footage is finalized + still
local, using the timing sidecar for the FL/FR<->SL/SR clock sync (no scan, no
delta-calibration). For each `score_added` in the game log it checks the high-fps
shot cam: did the CV see a made shot in the window? It writes the per-shot verdict
(`highlights.{logId}.validation`) and a game-level summary (`game.shot_qa`) flagging
disagreements (scored, but CV says miss/none).

SHADOW/QA ONLY — never mutates scores, cards, or plays. Best-effort: a missing
runtime / sidecar / footage / game-log just skips. Serial (one window at a time) so
there is no concurrency or OOM exposure.

Env:
    SHOT_QA_ENABLED             "true"/"false"  (default false — ships dormant)
    SHOT_VALIDATION_N_BEFORE_S  reaction look-back seconds (default 8)
    SHOT_VALIDATION_M_AFTER_S   forward margin seconds     (default 2)
    SHOT_VALIDATION_LATENCY_S   sidecar spawn->first-frame latency (default 0)
    SHOT_DET_WEIGHT / SHOT_DET_IMGSZ  detector weight / inference size (see detect.py)
"""
from __future__ import annotations

import json
import os
import time
from typing import List, Optional

from logging_service import get_logger

logger = get_logger("agx.shot_qa")


def enabled() -> bool:
    return os.getenv("SHOT_QA_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def _score_triggers(game: dict) -> List[dict]:
    """Scored makes from the game log -> [{ts, team, period, logId, points}].

    Only `score_added` (a new made basket). Requires a team (for cam selection),
    a timestamp, and a stable log id (to key the verdict)."""
    out: List[dict] = []
    for log in (game.get("logs") or []):
        if log.get("actionType") != "score_added":
            continue
        team, ts, log_id = log.get("team"), log.get("timestamp"), log.get("id")
        if not (team and ts and log_id):
            continue
        out.append({"ts": ts, "team": team, "period": log.get("period"),
                    "logId": str(log_id),
                    "points": (log.get("payload") or {}).get("points")})
    out.sort(key=lambda t: t["ts"])
    return out


def _prewarm(detector) -> None:
    """One throwaway inference so the game's first real window doesn't pay the
    one-time cuDNN autotune."""
    try:
        import numpy as np
        blank = [np.zeros((540, 720, 3), dtype=np.uint8) for _ in range(2)]
        detector._ball_track(blank)
    except Exception as e:  # noqa: BLE001
        logger.info("shot-qa prewarm skipped: %s", e)


def run_qa(fb, cfg, game_id: str, label: Optional[str],
           starting_side_team1: Optional[str]) -> Optional[dict]:
    """QA every scored make in one game. Returns the summary, or None if skipped."""
    if not enabled() or not label or not fb:
        return None
    t_start = time.time()
    try:
        from agx_pipeline.shot_detect.node import _VALIDATOR, _f
        from agx_pipeline.shot_detect.validate import validate_shot, write_validation
    except Exception as e:  # noqa: BLE001 — runtime missing => stay dark
        logger.warning("shot-qa runtime unavailable (%s) — skipping", e)
        return None
    try:
        session_dir = os.path.join(cfg.output_dir, label)
        sidecar_path = os.path.join(session_dir, f"{label}_shot_timing.json")
        if not os.path.exists(sidecar_path):
            logger.info("shot-qa: no sidecar %s — skip", sidecar_path)
            return None
        with open(sidecar_path) as fh:
            sidecar = json.load(fh)

        game = fb.get_game(game_id)
        if not game:
            logger.info("shot-qa: no game doc %s — skip", game_id)
            return None
        triggers = _score_triggers(game)
        if not triggers:
            logger.info("shot-qa: no scored makes in %s — skip", game_id)
            return None

        detector, rims = _VALIDATOR.get()
        _prewarm(detector)

        def video_for(angle: str) -> str:
            return os.path.join(session_dir, f"{label}_{angle}.mp4")

        n_before = _f("SHOT_VALIDATION_N_BEFORE_S", 8.0)
        m_after = _f("SHOT_VALIDATION_M_AFTER_S", 2.0)
        latency = _f("SHOT_VALIDATION_LATENCY_S", 0.0)

        n_confirmed = n_scored_verdict = 0
        disagreements: List[dict] = []
        for trig in triggers:
            val = validate_shot(detector, sidecar, video_for, trig,
                                starting_side_team1, rims,
                                n_before_s=n_before, m_after_s=m_after,
                                pipeline_latency_s=latency, stream=True)
            if val is None:            # side/anchor/footage missing for this one
                continue
            write_validation(fb, game_id, trig["logId"], val)
            n_scored_verdict += 1
            if val.get("agrees"):
                n_confirmed += 1
            else:
                disagreements.append({
                    "logId": trig["logId"], "team": trig["team"], "ts": trig["ts"],
                    "cam": val.get("cam"), "n_make": val.get("n_make"),
                    "n_miss": val.get("n_miss")})

        summary = {
            "n_scored": len(triggers),
            "n_with_verdict": n_scored_verdict,
            "n_confirmed": n_confirmed,
            "n_disagree": len(disagreements),
            "disagreements": disagreements[:50],
            "secs": round(time.time() - t_start, 1),
            "weight": os.path.basename(os.getenv("SHOT_DET_WEIGHT", "")) or None,
        }
        try:
            fb.db.collection("basketball-games").document(game_id).update(
                {"shot_qa": summary})
        except Exception as e:  # noqa: BLE001
            logger.warning("shot-qa summary write failed: %s", e)
        logger.info("shot-qa %s: %d scored, %d verdicts, %d confirmed, %d disagree (%.0fs)",
                    game_id, len(triggers), n_scored_verdict, n_confirmed,
                    len(disagreements), summary["secs"])
        return summary
    except Exception as e:  # noqa: BLE001 — QA must never break ingestion
        logger.warning("shot-qa failed: %s", e)
        return None

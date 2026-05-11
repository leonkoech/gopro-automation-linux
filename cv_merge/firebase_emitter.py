"""Firebase log emitter for CV-detected shots.

Emits GameLog entries shaped to match the existing `gopro-automation-wb`
`GameLog` TypeScript interface, so `plays_sync.create_plays_from_firebase_logs`
can consume them alongside human-entered logs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .merge import MergedShot

log = logging.getLogger("cv-merge.emitter")


def build_cv_shot_log(
    *,
    shot: MergedShot,
    team_label: str,
    team_name: str,
    game_start_iso: str,
    halftime_ts: Optional[float],
    cv_run_id: str,
    model_version: str,
    logged_by: str = "cv-pipeline",
) -> Dict[str, Any]:
    """Build one Firebase `logs[]` entry for a CV-detected shot.

    Per [UBA-217](https://linear.app/uball/issue/UBA-217), emits the same
    `actionType` values an operator would use:
      * made   → `score_added`  (handled by the existing `plays_sync.py`
                                branch — classifies as `2PT_MAKE`)
      * missed → `shot_missed`  (handled by the [UBA-237](
                                https://linear.app/uball/issue/UBA-237)
                                branch — classifies as `2PT_MISS`)

    V1 always emits `payload.points = 2` because the YOLOv11n V1 model
    is distance-blind; V2 will split 2/3pt when the model emits a
    distance estimate.

    CV provenance lives under `payload.{source,confidence,cv_run_id,
    model_version,side}` — plays_sync reads these to stamp the
    resulting Supabase `plays` row.
    """
    game_start = datetime.fromisoformat(str(game_start_iso).replace("Z", "+00:00"))
    shot_time = game_start + timedelta(seconds=shot.timestamp_seconds)
    period = "2nd" if (halftime_ts is not None and shot.timestamp_seconds >= halftime_ts) else "1st"

    if shot.outcome == "made":
        action_type = "score_added"
    elif shot.outcome == "missed":
        action_type = "shot_missed"
    else:
        raise ValueError(f"unknown shot outcome: {shot.outcome!r}")

    return {
        "actionType": action_type,
        "timestamp": shot_time.isoformat(),
        "loggedBy": logged_by,
        "team": team_label,            # "left" or "right" (hoop side)
        "teamName": team_name or "",
        "payload": {
            # V1 default: every shot is 2pt. V2 will set 3 here when the
            # fusion model emits a 3pt classification.
            "points": 2,
            # CV provenance — read by plays_sync to stamp source/confidence
            # on the resulting Supabase play row.
            "source": "cv",
            "confidence": round(shot.confidence, 4),
            "cv_run_id": cv_run_id,
            "model_version": model_version,
            "side": shot.side,         # "A" or "B"
        },
        # Fields required by the GameLog TS interface but not meaningful for CV.
        "gameTime": 0,
        "gameTimeFormatted": "00:00",
        "shotClock": 0,
        "period": period,
    }


def is_cv_already_emitted(firebase_game: Dict[str, Any]) -> bool:
    """Idempotency guard — skip if a previous merge run already stamped cv_emitted_at."""
    return bool(firebase_game.get("cv_emitted_at"))


def emit_cv_logs(
    firestore_db,
    firebase_game_id: str,
    logs: List[Dict[str, Any]],
    *,
    emit_target: str = "logs",
) -> None:
    """Append cv_shot entries to Firebase basketball-games/{id}.{emit_target}[].

    Also stamps `cv_emitted_at` so re-runs short-circuit via is_cv_already_emitted.
    """
    if not logs:
        log.info("no logs to emit for game %s", firebase_game_id)
        return

    # Import locally so the module can be imported in test contexts without
    # firebase_admin initialized.
    from firebase_admin import firestore

    ref = firestore_db.collection("basketball-games").document(firebase_game_id)
    ref.update({
        emit_target: firestore.ArrayUnion(logs),
        "cv_emitted_at": datetime.now(timezone.utc).isoformat(),
    })
    log.info(
        "emitted %d cv_shot entries to basketball-games/%s.%s",
        len(logs), firebase_game_id, emit_target,
    )

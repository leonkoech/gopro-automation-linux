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

    Shape matches `GameLog` in `gopro-automation-wb/src/types/game.ts` plus
    the new `actionType: "cv_shot"` variant consumed by `plays_sync.py`.
    """
    game_start = datetime.fromisoformat(str(game_start_iso).replace("Z", "+00:00"))
    shot_time = game_start + timedelta(seconds=shot.timestamp_seconds)
    period = "2nd" if (halftime_ts is not None and shot.timestamp_seconds >= halftime_ts) else "1st"

    return {
        "actionType": "cv_shot",
        "timestamp": shot_time.isoformat(),
        "loggedBy": logged_by,
        "team": team_label,            # "left" or "right"
        "teamName": team_name or "",
        "payload": {
            "outcome": shot.outcome,   # "made" or "missed"
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

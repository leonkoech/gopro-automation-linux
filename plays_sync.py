"""Create annotation-tool plays ("cards") from Firebase game logs.

This module is shared between the manual sync endpoint in `main.py` and the
automatic sync inside `video_processing.py`. It lives in its own module so both
callers can import it without introducing a circular dependency.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from logging_service import get_logger

logger = get_logger("gopro.plays_sync")


SHOT_LABELS: Dict[str, str] = {
    "2PT_MAKE": "2-Pointer Made",  "2PT_MISS": "2-Pointer Missed",
    "3PT_MAKE": "3-Pointer Made",  "3PT_MISS": "3-Pointer Missed",
    "FT_MAKE":  "Free Throw Made", "FT_MISS":  "Free Throw Missed",
    "FOUL":     "Foul",            "TIPOFF":   "Tipoff",
}


def create_plays_from_firebase_logs(
    client: Any,
    uball_game_id: str,
    firebase_game: Dict[str, Any],
) -> int:
    """Create plays in the annotation tool from a Firebase game's logs.

    Idempotent: if the annotation-tool game already has any plays, this is a
    no-op (returns 0) so re-runs of the pipeline do not duplicate cards.

    Args:
        client: A `UballClient` instance with `list_plays()` and `create_play()`.
        uball_game_id: The annotation-tool game UUID.
        firebase_game: The Firebase `basketball-games/{id}` document.

    Returns:
        Number of plays newly created (0 if skipped or none produced).
    """
    if not uball_game_id:
        return 0

    logs = firebase_game.get("logs", []) or []
    if not logs:
        return 0

    try:
        existing = client.list_plays(uball_game_id)
        if existing:
            logger.info(
                f"[PlaysSync] Game {uball_game_id} already has {len(existing)} plays — skipping."
            )
            return 0
    except Exception as exc:
        logger.warning(
            f"[PlaysSync] list_plays check failed for {uball_game_id}: {exc} — proceeding anyway."
        )

    created_at_raw = firebase_game.get("createdAt")
    if not created_at_raw:
        logger.warning(f"[PlaysSync] Firebase game missing createdAt — cannot compute play timestamps")
        return 0
    game_start = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))

    left_name = firebase_game.get("leftTeam", {}).get("name", "Team 1")
    right_name = firebase_game.get("rightTeam", {}).get("name", "Team 2")

    created = 0
    for log in logs:
        action = log.get("actionType", "")
        payload = log.get("payload", {}) or {}
        team_side = log.get("team")

        if action == "score_added":
            points = payload.get("points", 0)
            if points == 2:
                classification = "2PT_MAKE"
            elif points == 3:
                classification = "3PT_MAKE"
            elif points == 1:
                classification = "FT_MAKE"
            else:
                continue
        elif action == "foul_added":
            classification = "FOUL"
        elif action == "game_started":
            classification = "TIPOFF"
        else:
            continue

        if team_side == "left":
            team = "team1"
            team_name = left_name
        elif team_side == "right":
            team = "team2"
            team_name = right_name
        else:
            team = None
            team_name = "Game"

        log_ts_raw = log.get("timestamp")
        if not log_ts_raw:
            continue
        log_time = datetime.fromisoformat(log_ts_raw.replace("Z", "+00:00"))
        ts = (log_time - game_start).total_seconds()
        start_ts = max(0.0, ts - 5.0)
        end_ts = ts + 3.0

        note = f"{team_name} — {SHOT_LABELS.get(classification, classification)}"

        play_data: Dict[str, Any] = {
            "game_id": uball_game_id,
            "classification": classification,
            "note": note,
            "timestamp_seconds": ts,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
        }
        if team:
            play_data["team"] = team

        try:
            client.create_play(play_data)
            created += 1
        except Exception as exc:
            logger.warning(
                f"[PlaysSync] Failed to create play ({classification} at {ts:.1f}s) "
                f"for game {uball_game_id}: {exc}"
            )

    logger.info(f"[PlaysSync] Created {created}/{len(logs)} plays for game {uball_game_id}")
    return created

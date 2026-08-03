"""Create annotation-tool plays ("cards") from Firebase game logs.

This module is shared between the manual sync endpoint in `main.py` and the
automatic sync inside `video_processing.py`. It lives in its own module so both
callers can import it without introducing a circular dependency.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Optional

from logging_service import get_logger

logger = get_logger("gopro.plays_sync")


# Points → canonical annotation-tool classification. These MUST match the
# vocabulary the box score / UBall sync / chatbot expect: a 2-point make is
# FG_MAKE and a free throw is FREE_THROW_MAKE. (The old 2PT_MAKE/FT_MAKE were
# wrong and polluted the plays table; 4-pointers were dropped entirely.)
_MAKE_BY_POINTS: Dict[int, str] = {1: "FREE_THROW_MAKE", 2: "FG_MAKE", 3: "3PT_MAKE", 4: "4PT_MAKE"}
_MISS_BY_POINTS: Dict[int, str] = {1: "FREE_THROW_MISS", 2: "FG_MISS", 3: "3PT_MISS", 4: "4PT_MISS"}

SHOT_LABELS: Dict[str, str] = {
    "FG_MAKE": "Field Goal Made", "FG_MISS": "Field Goal Missed",
    "3PT_MAKE": "3-Pointer Made", "3PT_MISS": "3-Pointer Missed",
    "4PT_MAKE": "4-Pointer Made", "4PT_MISS": "4-Pointer Missed",
    "FREE_THROW_MAKE": "Free Throw Made", "FREE_THROW_MISS": "Free Throw Missed",
    "FOUL": "Foul", "TIPOFF": "Tipoff",
}

# Resolve the play's camera side (LEFT/RIGHT) from scoring team + period +
# startingSideTeam1 — the same halftime-aware math the highlight clips use.
# Imported defensively so this module still loads in the legacy (non-AGX) env.
try:
    from agx_pipeline.side_attribution import scoring_hoop_side as _scoring_hoop_side
except Exception:  # pragma: no cover - legacy env without agx_pipeline
    _scoring_hoop_side = None


def _cv_plays_enabled() -> bool:
    """Feature gate for CV-emitted plays reaching the annotation tool.

    V1 far-angle model under-detects shots in production (~3-12 detections per
    game vs an expected 70+), driven by a training-distribution mismatch with
    production cameras.  Until the far-angle retrain lands and validates on
    real games, CV-emitted events (those with `payload.source == "cv"`) are
    filtered out by `plays_sync` even if they end up in Firebase `logs[]`.

    Set CV_PLAYS_ENABLED=true on the Jetson .env to allow CV-source events to
    become Supabase plays.  Operator-entered scoreboard events are unaffected
    by this flag and always create plays.
    """
    return os.getenv("CV_PLAYS_ENABLED", "false").strip().lower() in ("true", "1", "yes", "on")


def create_plays_from_firebase_logs(
    client: Any,
    uball_game_id: str,
    firebase_game: Dict[str, Any],
    summary: Optional[Dict[str, Any]] = None,
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
            if summary is not None:
                summary.update({"created": 0, "with_players": 0,
                                "by_label": {}, "skipped_existing": True})
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
    cv_skipped = 0
    with_players = 0
    by_label: Dict[str, int] = {}
    cv_enabled = _cv_plays_enabled()
    for log in logs:
        action = log.get("actionType", "")
        payload = log.get("payload", {}) or {}
        team_side = log.get("team")

        # Feature gate: CV-source events (payload.source == "cv") are filtered
        # out until CV_PLAYS_ENABLED=true.  Defense-in-depth with cv-merge's
        # emit_target=cv_logs_staging shadow gate — even if CV events end up
        # in logs[] somehow, plays_sync still won't promote them while the
        # far-angle model is under-detecting on production cameras.
        if payload.get("source") == "cv" and not cv_enabled:
            cv_skipped += 1
            continue

        # Player pre-fill: only a player tap (player_score_added) carries a
        # player; a team-button score (score_added) does not, and the annotator
        # fills it in later — but the card + clip are already placed for them.
        player_id = None
        player_name = None
        if action in ("score_added", "player_score_added"):
            classification = _MAKE_BY_POINTS.get(payload.get("points", 0))
            if not classification:
                continue  # 0-point / malformed score log
            if action == "player_score_added":
                player_id = payload.get("playerId")
                player_name = payload.get("playerName")
        elif action == "shot_missed":
            # UBA-237: V1 missed shots via the CV pipeline. `payload.points`
            # chooses the distance bucket (V1 always sets 2).
            classification = _MISS_BY_POINTS.get(payload.get("points", 2))
            if not classification:
                logger.warning(f"[PlaysSync] shot_missed log has invalid points: {payload}")
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

        is_cv = payload.get("source") == "cv"
        label = SHOT_LABELS.get(classification, classification)

        # Camera side (LEFT/RIGHT) the play happened at — flips at halftime.
        # Same math as the highlight clips; None if the resolver isn't available
        # (legacy env) or the event has no team (e.g. tipoff).
        angle = None
        if _scoring_hoop_side and team_side in ("left", "right"):
            hoop = _scoring_hoop_side(team_side, log.get("period"),
                                      firebase_game.get("startingSideTeam1"))
            angle = hoop.upper() if hoop else None

        confidence = 1.0  # operator-entered scoreboard truth
        if is_cv:
            c = payload.get("confidence")
            confidence = float(c) if c is not None else None

        if is_cv:
            # V1 CV is distance-blind — don't claim a specific shot value.
            cv_word = ("Made" if classification.endswith("_MAKE")
                       else "Missed" if classification.endswith("_MISS") else label)
            note = f"{team_name} — {cv_word} (CV)"
        else:
            note = f"{player_name or team_name} — {label}"

        play_data: Dict[str, Any] = {
            "game_id": uball_game_id,
            "classification": classification,
            "note": note,
            "timestamp_seconds": ts,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            # source tags the card's origin so annotators can tell auto-seeded
            # scoreboard plays ("firebase") from CV ("cv") and their own ("manual").
            "source": "cv" if is_cv else "firebase",
            "events": [{
                "label": classification,
                "playerA": player_name, "playerAId": player_id,
                "playerB": None, "playerBId": None,
                "confidence": confidence,
            }],
        }
        if team:
            play_data["team"] = team
        if angle:
            play_data["angle"] = angle
        if player_id or player_name:
            play_data["player_a"] = player_name
            play_data["player_a_id"] = player_id
        if confidence is not None:
            play_data["confidence"] = confidence

        try:
            client.create_play(play_data)
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
            if player_id:
                with_players += 1
        except Exception as exc:
            logger.warning(
                f"[PlaysSync] Failed to create play ({classification} at {ts:.1f}s) "
                f"for game {uball_game_id}: {exc}"
            )

    if cv_skipped:
        logger.info(
            f"[PlaysSync] Created {created}/{len(logs)} plays for game {uball_game_id} "
            f"(skipped {cv_skipped} CV-source event(s); CV_PLAYS_ENABLED={cv_enabled})"
        )
    else:
        logger.info(f"[PlaysSync] Created {created}/{len(logs)} plays for game {uball_game_id}")
    if summary is not None:
        summary.update({"created": created, "with_players": with_players,
                        "by_label": by_label, "skipped_existing": False})
    return created

"""Create annotation-tool plays ("cards") from Firebase game logs.

This module is shared between the manual sync endpoint in `main.py` and the
automatic sync inside `video_processing.py`. It lives in its own module so both
callers can import it without introducing a circular dependency.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from logging_service import get_logger

logger = get_logger("gopro.plays_sync")


# Points → canonical annotation-tool classification. These MUST match the
# vocabulary the box score / UBall sync / chatbot expect: a 2-point make is
# FG_MAKE and a free throw is FREE_THROW_MAKE. (The old 2PT_MAKE/FT_MAKE were
# wrong and polluted the plays table; 4-pointers were dropped entirely.)
_MAKE_BY_POINTS: Dict[int, str] = {1: "FREE_THROW_MAKE", 2: "FG_MAKE", 3: "3PT_MAKE", 4: "4PT_MAKE"}

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
    with_players = 0
    by_label: Dict[str, int] = {}
    for log in logs:
        action = log.get("actionType", "")
        payload = log.get("payload", {}) or {}
        team_side = log.get("team")

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
        note = f"{player_name or team_name} — {label}"

        play_data: Dict[str, Any] = {
            "game_id": uball_game_id,
            "classification": classification,
            "note": note,
            "timestamp_seconds": ts,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            # source tags the card's origin so annotators can tell auto-seeded
            # scoreboard plays ("firebase") from their own ("manual").
            "source": "firebase",
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

    logger.info(f"[PlaysSync] Created {created}/{len(logs)} plays for game {uball_game_id}")
    if summary is not None:
        summary.update({"created": created, "with_players": with_players,
                        "by_label": by_label, "skipped_existing": False})
    return created


_HOOP_ANGLE = {"left": "LEFT", "right": "RIGHT"}

# The typing stage (LiveTyper) writes its verdict to
# basketball-games/{game}.cv_points.{cv_<epoch>_<side>} -- the SAME key the clip
# cutter mints in shot_detect/live.py. Joining on it turns a bare make/miss into
# a real shot type. ~85% of shots carry an entry; the rest fall back to FG,
# which is what every CV card used to be.
_TYPE_BY_POINTS = {1: ("FREE_THROW_MAKE", "FREE_THROW_MISS"),
                   2: ("FG_MAKE", "FG_MISS"),
                   3: ("3PT_MAKE", "3PT_MISS"),
                   4: ("4PT_MAKE", "4PT_MISS")}


def _cv_key(shot: Dict[str, Any]) -> Optional[str]:
    """Rebuild the cv_<epoch>_<side> key for a shot_live entry."""
    wc, side = shot.get("wallclock"), shot.get("side")
    if not wc or side not in ("left", "right"):
        return None
    try:
        epoch = datetime.fromisoformat(str(wc).replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        return None
    return f"cv_{int(epoch)}_{side}"


def _classify(shot: Dict[str, Any], cv_points: Dict[str, Any]) -> tuple:
    """(classification, typed) -- typed says whether a real shot type was found."""
    made = bool(shot.get("made"))
    key = _cv_key(shot)
    rec = cv_points.get(key) if key else None
    pts = (rec or {}).get("points")
    pair = _TYPE_BY_POINTS.get(pts)
    if pair:
        return (pair[0] if made else pair[1]), True
    return ("FG_MAKE" if made else "FG_MISS"), False



def create_plays_from_shot_live(
    client: Any,
    uball_game_id: str,
    firebase_game: Dict[str, Any],
    dry_run: bool = False,
    summary: Optional[Dict[str, Any]] = None,
) -> int:
    """Create annotation cards from the CV's `shot_live` shadow — every make/miss
    the high-fps SL/SR detector saw, independent of the scorekeeper.

    Tagged `source="cv"` so annotators can tell these from scoreboard cards AND so
    this is idempotent on CV cards (re-runs won't duplicate). The CV knows the hoop
    (angle LEFT/RIGHT), make/miss, and -- where the typing stage got to it -- the
    point value, joined from cv_points on the shared cv_<epoch>_<side> key. A shot
    with no typed record still falls back to FG_MAKE/FG_MISS. Team is left for the
    annotator. Low confidence (0.5) flags
    them as CV-suggested + review-needed (~65% recall / ~80% precision shadow).

    `dry_run` counts without writing. Returns the number created (or would create).
    """
    if not uball_game_id:
        return 0
    live = firebase_game.get("shot_live") or {}
    shots = live.get("shots") or []
    cv_points = firebase_game.get("cv_points") or {}
    if not shots:
        return 0

    # Idempotency: skip if this game already has CV cards.
    try:
        existing = client.list_plays(uball_game_id)
        if any((p.get("source") == "cv") for p in existing):
            logger.info(f"[PlaysSync/CV] Game {uball_game_id} already has CV cards — skipping.")
            if summary is not None:
                summary.update({"created": 0, "by_label": {}, "skipped_existing": True})
            return 0
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[PlaysSync/CV] list_plays check failed for {uball_game_id}: {exc}")

    created_at_raw = firebase_game.get("createdAt")
    game_start = (datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                  if created_at_raw else None)

    created = 0
    by_label: Dict[str, int] = {}
    for s in shots:
        classification, typed = _classify(s, cv_points)
        # Video-timeline seconds: prefer wallclock - game_start; fall back to the
        # segment offset. Approximate (SL/SR vs tracking-cam sync) — the annotator
        # nudges it; the card + rough position is what saves them the work.
        ts = None
        wc = s.get("wallclock")
        if wc and game_start:
            try:
                ts = (datetime.fromisoformat(wc) - game_start).total_seconds()
            except Exception:  # noqa: BLE001
                ts = None
        if ts is None:
            ts = float(s.get("seg", 0)) * 4.0 + float(s.get("t_shot", 0.0))
        ts = max(0.0, ts)
        angle = _HOOP_ANGLE.get(s.get("side"))
        label = SHOT_LABELS.get(classification, classification)
        note = f"CV: {label}" + (f" ({s.get('cam')} · {s.get('side')} rim)" if s.get("cam") else "")

        play_data: Dict[str, Any] = {
            "game_id": uball_game_id,
            "classification": classification,
            "note": note,
            "timestamp_seconds": ts,
            "start_timestamp": max(0.0, ts - 5.0),
            "end_timestamp": ts + 3.0,
            "source": "cv",
            "confidence": 0.5,
            "events": [{
                "label": classification,
                "playerA": None, "playerAId": None,
                "playerB": None, "playerBId": None,
                "confidence": 0.5,
            }],
        }
        if angle:
            play_data["angle"] = angle

        if dry_run:
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
            continue
        try:
            client.create_play(play_data)
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[PlaysSync/CV] create_play failed ({classification} @ {ts:.1f}s): {exc}")

    logger.info(f"[PlaysSync/CV] {'(dry-run) ' if dry_run else ''}created {created}/{len(shots)} "
                f"CV cards for game {uball_game_id}")
    if summary is not None:
        summary.update({"created": created, "by_label": by_label, "skipped_existing": False})
    return created

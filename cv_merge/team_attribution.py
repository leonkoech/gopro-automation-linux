"""Team attribution for merged CV shots.

Algorithm (V1 — full-court, hoop-side based):
  1. Each side of camera pair watches a fixed hoop:
       Side A (FR+NR) -> "right" hoop
       Side B (FL+NL) -> "left" hoop
  2. At tip-off, team1 attacks `startingSideTeam1` hoop.
  3. At halftime (first `period_changed` log with newValue='2nd'), teams flip.
  4. For a shot at time T from side S:
       - hoop_watched = SIDE_TO_HOOP[S]
       - team1_is_attacking_that_hoop = (team1_hoop_now == hoop_watched)
       - Firebase log label: 'left' if team1, else 'right'

If `startingSideTeam1` is missing, fall back to team1='left' (implicit
default) and let the caller flag the game needs_review.

Pure functions only — unit-testable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


# Firebase GameLog convention: team1 -> 'left', team2 -> 'right'.
TEAM1_FB_LABEL = "left"
TEAM2_FB_LABEL = "right"

# Physical camera layout confirmed by the user:
#   FR + NR point at the right hoop (Side A).
#   FL + NL point at the left hoop (Side B).
SIDE_TO_HOOP = {"A": "right", "B": "left"}


def _opposite_hoop(hoop: str) -> str:
    return "right" if hoop == "left" else "left"


def find_halftime_seconds(firebase_game: Dict[str, Any]) -> Optional[float]:
    """Return seconds-from-game-start when halftime occurred, or None.

    Looks at the first `period_changed` log with `payload.newValue == '2nd'`.
    """
    created_at = firebase_game.get("createdAt")
    if not created_at:
        return None
    try:
        game_start = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except ValueError:
        return None

    for log in firebase_game.get("logs", []) or []:
        if log.get("actionType") != "period_changed":
            continue
        payload = log.get("payload", {}) or {}
        if payload.get("newValue") != "2nd":
            continue
        log_ts = log.get("timestamp")
        if not log_ts:
            continue
        try:
            log_dt = datetime.fromisoformat(str(log_ts).replace("Z", "+00:00"))
        except ValueError:
            continue
        return (log_dt - game_start).total_seconds()
    return None


def attribute_team(
    *,
    side: str,
    timestamp_seconds: float,
    starting_side_team1: Optional[str],
    halftime_ts: Optional[float],
) -> str:
    """Return 'left' (team1) or 'right' (team2) for a CV-detected shot.

    Args:
        side: 'A' or 'B' — which camera pair observed the shot.
        timestamp_seconds: offset from game start.
        starting_side_team1: 'left' or 'right' — which hoop team1 attacks at
            tip-off. If None, falls back to 'left' (team1 attacks left hoop).
        halftime_ts: seconds from game start when halftime occurred. If None,
            the entire game is treated as the first half.

    Returns:
        'left' if team1 took the shot, 'right' if team2.
    """
    hoop_watched = SIDE_TO_HOOP.get(side)
    if hoop_watched is None:
        raise ValueError(f"unknown side: {side!r}")

    starting = starting_side_team1 if starting_side_team1 in ("left", "right") else "left"
    is_second_half = halftime_ts is not None and timestamp_seconds >= halftime_ts
    team1_hoop_now = _opposite_hoop(starting) if is_second_half else starting

    return TEAM1_FB_LABEL if hoop_watched == team1_hoop_now else TEAM2_FB_LABEL

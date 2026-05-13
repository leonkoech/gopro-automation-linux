"""Team attribution for merged CV shots.

V1 — simplified (UBA-214 update 2026-04-23):

The Firebase log `team` field is the **hoop side** the shot occurred at
(`"left"` or `"right"`). `plays_sync.py` then maps it via
`game.leftTeam.name` / `game.rightTeam.name` — so the CV layer doesn't
need to track which team is on which hoop, or model the halftime flip.

For each merged shot:

* If the upstream fusion run already stamped `hoop_side` on the shot
  (which Phase 3.6 / UBA-238 does in deploy/entrypoint.py for the
  fusion container), use it as-is.
* Otherwise fall back to deriving from `side` (A=right, B=left) — same
  physical layout, just less explicit. Lets the merge container keep
  working against older `detection_results.json` payloads that
  pre-date UBA-238.

The legacy `attribute_team()` function (which tracked
`startingSideTeam1` + halftime to map hoop→team1/team2) is kept for
backward-compatibility with any caller that still needs it, but it is
no longer the merge container's default path. Mark it as deprecated
in docstring and leave the unit tests in place for the algorithm.

Pure functions only — unit-testable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


# Firebase GameLog convention for the V1 simplified attribution:
# the `team` field is the hoop side, mapped downstream by plays_sync.
HOOP_SIDE_LEFT = "left"
HOOP_SIDE_RIGHT = "right"

# Physical camera layout confirmed by the user:
#   FR + NR point at the right hoop (Side A).
#   FL + NL point at the left hoop (Side B).
SIDE_TO_HOOP = {"A": HOOP_SIDE_RIGHT, "B": HOOP_SIDE_LEFT}


def hoop_side_for_shot(
    *,
    side: str,
    shot_source: Optional[Dict[str, Any]] = None,
) -> str:
    """Return ``"left"`` or ``"right"`` — the hoop the shot occurred at.

    Resolution order:

    1. ``shot_source["hoop_side"]`` if present and valid — preferred path
       once Phase 3.6 (UBA-238) ships the per-shot stamping in
       ``Uball_dual_angle_fusion/deploy/entrypoint.py``.
    2. Side-derived fallback — A→right, B→left. Works against any
       older ``detection_results.json`` payload that pre-dates UBA-238.

    The merged-shot pipeline calls this with the source dict that the
    fusion container produced so we don't have to plumb both the side
    and the optional pre-stamped value through every caller.
    """
    if isinstance(shot_source, dict):
        explicit = shot_source.get("hoop_side")
        if explicit in (HOOP_SIDE_LEFT, HOOP_SIDE_RIGHT):
            return explicit

    hoop = SIDE_TO_HOOP.get(side)
    if hoop is None:
        raise ValueError(f"unknown side: {side!r}")
    return hoop


# ---------------------------------------------------------------------------
# Legacy halftime-aware attribution — retained for callers that still want
# team1/team2 resolution at the CV layer. NOT used by the V1 merge path.
# ---------------------------------------------------------------------------
# Firebase GameLog convention for the OLD logic: team1 -> 'left',
# team2 -> 'right'.
TEAM1_FB_LABEL = "left"
TEAM2_FB_LABEL = "right"


def _opposite_hoop(hoop: str) -> str:
    return "right" if hoop == "left" else "left"


def find_halftime_seconds(firebase_game: Dict[str, Any]) -> Optional[float]:
    """Return seconds-from-game-start when halftime occurred, or None.

    Looks at the first `period_changed` log with `payload.newValue == '2nd'`.

    Kept for completeness / future V2 work — V1 attribution no longer
    consults this (per UBA-214 simplification 2026-04-23).
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
    """**Deprecated for V1.** Returns 'left' (team1) or 'right' (team2).

    This is the original Phase 3 design that tracked halftime + starting
    side to map hoop→team. The V1 merge path now uses
    :func:`hoop_side_for_shot` instead and lets ``plays_sync.py`` do the
    team mapping downstream.

    Retained here for backward-compatibility callers + the
    halftime-tracking unit tests. Will be removed when no caller in the
    repo references it.
    """
    hoop_watched = SIDE_TO_HOOP.get(side)
    if hoop_watched is None:
        raise ValueError(f"unknown side: {side!r}")

    starting = starting_side_team1 if starting_side_team1 in ("left", "right") else "left"
    is_second_half = halftime_ts is not None and timestamp_seconds >= halftime_ts
    team1_hoop_now = _opposite_hoop(starting) if is_second_half else starting

    return TEAM1_FB_LABEL if hoop_watched == team1_hoop_now else TEAM2_FB_LABEL

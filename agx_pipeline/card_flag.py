"""Green / yellow / red flag for a CV annotation card: how far an annotator can trust it.

  GREEN   the shot type is known with the shooter's feet clearly inside a zone (at least LINE_PX
          from the 3PT and 4PT lines; a free throw needs no margin) AND a jersey was suggested
          from a number read in the shot clip itself
  YELLOW  both answers are there but one is shaky: the feet were within LINE_PX of a line, or the
          number was only found by following the player back in time
  RED     not a game shot: the game clock was paused (scoreboard timer), or the shot sits in a
          burst of shooting at one basket (warm-ups, timeouts, post-game: BURST_* below, counted
          over makes AND misses); or the type or the jersey is missing

Measured on 137 real shots of game d6539161 (2026-09-15): green 57% of cards (type right 91%,
jersey right 88%), yellow 24% (76% / 85%), red 19%.

`v` is the tracker's cv_points entry (zone, line_px, who_from); `suggestion` is what the card
shows, (number, name) or None. The flag goes on the card's first event (`cv_flag`,
`cv_flag_reason`) and its confidence is set from CONFIDENCE, so the editor's older two-colour
badge (green at >= 0.7) still agrees.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

LINE_PX = 25.0
CONFIDENCE = {"green": 0.9, "yellow": 0.6, "red": 0.3}
# Burst rule for cards, tuned on games d6539161 + 11850fc1 (all CV shots): red on 80% of the cards
# that were not game shots (400/499) and on 1 of 259 real shots.
BURST_WINDOW_S = 45.0
BURST_CORE_MIN = 8
BURST_EDGE_S = 10.0


def card_flag(v: Optional[Dict[str, Any]], suggestion: Optional[tuple],
              paused: bool = False, burst: bool = False) -> Tuple[str, str]:
    """(flag, one-line reason for the annotator)."""
    if paused:
        return "red", "shot while the game clock was paused"
    if burst:
        return "red", "looks like warm-up or stoppage shooting (a burst of shots at one basket)"
    v = v or {}
    zone = v.get("zone")
    if not zone and not suggestion:
        return "red", "no shot type and no jersey"
    if not zone:
        return "red", "no shot type"
    if not suggestion:
        return "red", "no jersey"
    try:
        near = zone != "FREE_THROW" and v.get("line_px") is not None and abs(float(v["line_px"])) < LINE_PX
    except (TypeError, ValueError):
        near = False
    if near:
        return "yellow", "shooter's feet were close to a line"
    if v.get("who_from") == "timeline":
        return "yellow", "jersey found only after the player was followed back"
    return "green", "shot type and jersey both clear"


def burst_shots(shot_keys: Iterable[str]) -> Set[str]:
    """Card ids (cv_<epoch>_<side>, every CV shot of the game) inside a shooting burst."""
    from agx_pipeline.deadball import dense_shooting
    return dense_shooting(shot_keys, BURST_WINDOW_S, BURST_CORE_MIN, BURST_EDGE_S)


def paused_intervals(logs: List[Dict[str, Any]]) -> Optional[List[Tuple[float, float]]]:
    """[(start, end)] epochs when the scoreboard clock was NOT running, between game start and
    end, from the scorekeeper's timer_started / timer_paused log; None when the game logged no
    timer use at all (then nothing is marked paused)."""
    from datetime import datetime

    def ep(x):
        try:
            return datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp()
        except Exception:  # noqa: BLE001
            return None

    ev = sorted((ep(l.get("timestamp")), l.get("actionType")) for l in (logs or [])
                if l.get("actionType") in ("timer_started", "timer_paused", "game_started", "game_ended"))
    ev = [(t, a) for t, a in ev if t is not None]
    if not any(a == "timer_started" for _, a in ev):
        return None
    out, paused_since = [], None
    for t, a in ev:
        if a in ("game_started", "timer_paused") and paused_since is None:
            paused_since = t
        elif a == "timer_started" and paused_since is not None:
            out.append((paused_since, t))
            paused_since = None
        elif a == "game_ended" and paused_since is not None:
            out.append((paused_since, t))
            paused_since = None
    return out


def is_paused(intervals: Optional[List[Tuple[float, float]]], epoch: Optional[float]) -> bool:
    return bool(intervals) and epoch is not None and any(a <= epoch <= b for a, b in intervals)

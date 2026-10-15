"""Measured confidence for each answer on a CV card, and the card's flag from them.

Every number below is the accuracy that answers of that kind had on annotated games: 7 games,
901 paired shots, measured 2026-10-14 (Laplace-smoothed). Held out one game at a time, cards
whose every field was >= GREEN_MIN were fully right 82% of the time (37% of cards), and the
product of the fields was slightly cautious (cards predicted 70-80% were right 80%).

  make/miss  what the detector called
  type       how far the shooter's feet were from the nearest 3PT / 4PT line (free throws apart)
  team       how clearly the jersey numbers agreed on the half-time switch (tracker_teams)
  shooter    where the suggested number came from: read in the clip (by vote strength), found
             by following the player back, or the appearance model (by its confidence)

A field with no answer has confidence 0. The flag is set by the weakest field.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

GREEN_MIN = 0.85
YELLOW_MIN = 0.65

MAKE_MISS = {True: 0.98, False: 0.95}
TYPE_FT = 0.95
TYPE_BY_LINE_PX = ((50.0, 0.93), (25.0, 0.88), (0.0, 0.72))     # |line_px| >= key -> confidence
TYPE_NO_LINE = 0.85
TEAM_NUMBERS_CLEAR, TEAM_NUMBERS_WEAK, TEAM_COLOUR, TEAM_AGREE_MIN = 0.95, 0.80, 0.83, 0.6
SHOOTER_READ_STRONG, SHOOTER_READ_OTHER = 0.90, 0.72
READ_STRONG_VOTES, READ_STRONG_RATIO = 6.0, 3.0
SHOOTER_FOLLOW_BACK = 0.51
SHOOTER_LOOKS = ((0.8, 0.83), (0.6, 0.64))                       # looks conf >= key -> confidence

FIELD_WORDS = {"makemiss": "make / miss", "type": "shot type", "team": "team", "shooter": "shooter"}


def type_confidence(v: Dict[str, Any]) -> float:
    zone = v.get("zone")
    if not zone:
        return 0.0
    if zone == "FREE_THROW":
        return TYPE_FT
    try:
        lp = abs(float(v["line_px"]))
    except (KeyError, TypeError, ValueError):
        return TYPE_NO_LINE
    return next(c for edge, c in TYPE_BY_LINE_PX if lp >= edge)


def team_confidence(tracker_teams: Optional[Dict[str, Any]]) -> float:
    if not tracker_teams:
        return 0.0
    if tracker_teams.get("source") == "numbers":
        try:
            clear = float(tracker_teams.get("number_agreement") or 0) >= TEAM_AGREE_MIN
        except (TypeError, ValueError):
            clear = False
        return TEAM_NUMBERS_CLEAR if clear else TEAM_NUMBERS_WEAK
    return TEAM_COLOUR


def shooter_confidence(v: Dict[str, Any], suggestion: Optional[tuple]) -> float:
    if not suggestion:
        return 0.0
    if len(suggestion) > 2:                                      # named by appearance
        try:
            c = float((v.get("who_looks") or {}).get("conf") or 0)
        except (TypeError, ValueError, AttributeError):
            c = 0.0
        return next((conf for edge, conf in SHOOTER_LOOKS if c >= edge), 0.0)
    if v.get("who_from") == "timeline":
        return SHOOTER_FOLLOW_BACK
    votes = sorted((float(w) for w in (v.get("who_votes") or {}).values()), reverse=True) if isinstance(
        v.get("who_votes"), dict) else []
    top = votes[0] if votes else 0.0
    ratio = top / votes[1] if len(votes) > 1 and votes[1] > 0 else float("inf")
    return SHOOTER_READ_STRONG if top >= READ_STRONG_VOTES and ratio >= READ_STRONG_RATIO else SHOOTER_READ_OTHER


def card_confidence(v: Optional[Dict[str, Any]], suggestion: Optional[tuple], made: bool,
                    tracker_teams: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """Per-field confidence (0..1) and their product, the chance the whole card is right."""
    v = v or {}
    out = {"makemiss": MAKE_MISS[bool(made)], "type": type_confidence(v),
           "team": team_confidence(tracker_teams), "shooter": shooter_confidence(v, suggestion)}
    card = 1.0
    for x in out.values():
        card *= x
    return {k: round(x, 2) for k, x in dict(out, card=card).items()}


def flag_from_confidence(conf: Dict[str, float], paused: bool = False,
                         burst: bool = False) -> Tuple[str, str]:
    """(flag, one-line reason) from the weakest field; a paused clock or a shooting burst is red."""
    if paused:
        return "red", "shot while the game clock was paused"
    if burst:
        return "red", "looks like warm-up or stoppage shooting (a burst of shots at one basket)"
    fields = {k: x for k, x in conf.items() if k != "card"}
    low = min(fields, key=fields.get)
    if fields[low] <= 0.0:
        return "red", "no %s" % FIELD_WORDS[low]
    if fields[low] >= GREEN_MIN:
        return "green", "every answer is %d%% or better" % round(100 * fields[low])
    level = "yellow" if fields[low] >= YELLOW_MIN else "red"
    return level, "check the %s (%d%% sure)" % (FIELD_WORDS[low], round(100 * fields[low]))

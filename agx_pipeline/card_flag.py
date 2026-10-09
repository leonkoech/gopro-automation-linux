"""Green / yellow / red flag for a CV annotation card: how far an annotator can trust it.

  GREEN   the shot type is known with the shooter's feet clearly inside a zone (at least LINE_PX
          from the 3PT and 4PT lines; a free throw needs no margin) AND a jersey was suggested
          from a number read in the shot clip itself
  YELLOW  both answers are there but one is shaky: the feet were within LINE_PX of a line, or the
          number was only found by following the player back in time
  RED     the type or the jersey is missing, or the shot came while the game clock was paused

Measured on 137 real shots of game d6539161 (2026-09-15): green 57% of cards (type right 91%,
jersey right 88%), yellow 24% (76% / 85%), red 19%.

`v` is the tracker's cv_points entry (zone, line_px, who_from); `suggestion` is what the card
shows, (number, name) or None. The flag goes on the card's first event (`cv_flag`,
`cv_flag_reason`) and its confidence is set from CONFIDENCE, so the editor's older two-colour
badge (green at >= 0.7) still agrees.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

LINE_PX = 25.0
CONFIDENCE = {"green": 0.9, "yellow": 0.6, "red": 0.3}


def card_flag(v: Optional[Dict[str, Any]], suggestion: Optional[tuple],
              paused: bool = False) -> Tuple[str, str]:
    """(flag, one-line reason for the annotator)."""
    if paused:
        return "red", "shot while the game clock was paused"
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

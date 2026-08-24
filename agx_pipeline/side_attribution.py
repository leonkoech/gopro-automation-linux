"""Physical scoring-side resolution for side-aware highlight clips (issue #4).

Basketball teams switch ends at halftime. The scoreboard operator scores by
TEAM identity — the left button is always team1, the right button always team2 —
so a score log's ``team`` field ("left"/"right") names the team, not the physical
hoop. To route a highlight clip (or a play's camera ``angle``) to the camera that
actually filmed the basket, we translate:

    scoring team + period + startingSideTeam1  ->  physical hoop side (left/right)

which flips at halftime. Left-hoop cameras are FL/NL, right-hoop cameras FR/NR.
Pure functions only — unit-testable, no I/O.
"""

from __future__ import annotations

from typing import Optional

LEFT = "left"
RIGHT = "right"

# Physical camera layout: FL/NL film the left hoop, FR/NR the right hoop.
HOOP_CAMERAS = {LEFT: ("FL", "NL"), RIGHT: ("FR", "NR")}


def _opposite(side: str) -> str:
    return RIGHT if side == LEFT else LEFT


def team1_attacking_side(
    period: Optional[str], starting_side_team1: Optional[str]
) -> str:
    """Which hoop team1 attacks in ``period`` ("left"/"right").

    ``starting_side_team1`` is the hoop team1 attacks at tip-off, set by the
    operator at check-in; it flips in the 2nd half. Defaults to "left" when
    unknown.
    """
    starting = starting_side_team1 if starting_side_team1 in (LEFT, RIGHT) else LEFT
    return _opposite(starting) if period == "2nd" else starting


def scoring_hoop_side(
    team: Optional[str],
    period: Optional[str],
    starting_side_team1: Optional[str],
) -> Optional[str]:
    """Physical hoop side ("left"/"right") where ``team`` scored, or ``None``.

    ``team`` is the scoreboard button / team identity: "left" == team1,
    "right" == team2. Returns ``None`` when ``team`` is not a known side, so the
    caller can fall back to the default (side-agnostic) clip behaviour instead
    of guessing a camera.
    """
    if team not in (LEFT, RIGHT):
        return None
    t1 = team1_attacking_side(period, starting_side_team1)
    return t1 if team == LEFT else _opposite(t1)

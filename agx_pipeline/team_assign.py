"""Which team took each CV shot — from the basket and the half, even when no player is named.

Within a half every shot at one basket belongs to the team attacking it; teams swap baskets once,
at half-time. The possession tracker records the shooter's kit colour on each made-shot clip
(cv_points.{logId}.kit_lab). From those, per game:
  1. find the one half-time moment that best splits the shots into two colour groups:
     A = left basket before it + right basket after it, B = the rest (or no switch at all,
     if that separates the colours better)
  2. name the groups by the cheaper assignment to the game's two registered jersey colours —
     relative, so it holds even when a registered colour is off (it was on 1 of 3 test games)
Measured on 395 annotated shots of three games: team right on 392 (99%).

The result is stored once on the game doc as `tracker_teams`
({"switch_epoch": float|None, "left_basket_first": "left"|"right"}); team_for_shot() turns any
shot (make or miss) into "left"/"right" from its basket side and time.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import numpy as np

_ID = re.compile(r"^cv_(\d{9,11})_(left|right)$")
MIN_SHOTS = 6


def _hex_lab(h: str) -> Optional[np.ndarray]:
    try:
        import cv2
        rgb = np.array([[[int(h[1:3], 16), int(h[3:5], 16), int(h[5:7], 16)]]], np.uint8)
        return cv2.cvtColor(rgb, cv2.COLOR_RGB2LAB)[0, 0].astype(float)
    except Exception:  # noqa: BLE001
        return None


def _sep(A: List[np.ndarray], B: List[np.ndarray]) -> float:
    if len(A) < 3 or len(B) < 3:
        return -1.0
    return float(np.linalg.norm(np.median(A, axis=0) - np.median(B, axis=0))
                 * np.sqrt(len(A) * len(B) / (len(A) + len(B))))


def solve(shots: List[Tuple[float, str, List[float]]], left_colour: Optional[str],
          right_colour: Optional[str]) -> Optional[Dict]:
    """shots: (epoch, basket side 'left'/'right', kit Lab). Returns tracker_teams or None."""
    shots = [s for s in shots if s[2] is not None]
    if len(shots) < MIN_SHOTS:
        return None
    times = sorted(s[0] for s in shots)
    cands = [None] + [(times[i - 1] + times[i]) / 2 for i in range(3, len(times) - 3)]
    best = (-1.0, None)
    for T in cands:
        in_a = lambda s: (s[1] == "left") == (T is None or s[0] < T)
        A = [np.array(s[2]) for s in shots if in_a(s)]
        B = [np.array(s[2]) for s in shots if not in_a(s)]
        sc = _sep(A, B)
        if sc > best[0]:
            best = (sc, T)
    T = best[1]
    in_a = lambda s: (s[1] == "left") == (T is None or s[0] < T)
    mA = np.median([s[2] for s in shots if in_a(s)], axis=0)
    mB = np.median([s[2] for s in shots if not in_a(s)], axis=0)
    L, R = _hex_lab(left_colour or ""), _hex_lab(right_colour or "")
    if L is None or R is None:
        return None
    straight = np.linalg.norm(mA - L) + np.linalg.norm(mB - R)
    swapped = np.linalg.norm(mA - R) + np.linalg.norm(mB - L)
    # group A attacks the LEFT basket first
    return {"switch_epoch": T, "left_basket_first": "left" if straight <= swapped else "right"}


def team_for_shot(tt: Optional[Dict], side: Optional[str], epoch: Optional[float]) -> Optional[str]:
    """'left'/'right' (the game's leftTeam/rightTeam) for a shot at `side` basket at `epoch`."""
    if not tt or side not in ("left", "right") or epoch is None:
        return None
    first = tt.get("left_basket_first")
    if first not in ("left", "right"):
        return None
    other = "right" if first == "left" else "left"
    T = tt.get("switch_epoch")
    before = T is None or epoch < T
    attacking_left = first if before else other
    return attacking_left if side == "left" else ("right" if attacking_left == "left" else "left")


def assign_game(game: Dict) -> Optional[Dict]:
    """tracker_teams for a Firebase game doc from its cv_points kit colours."""
    shots = []
    for log_id, v in (game.get("cv_points") or {}).items():
        m = _ID.match(str(log_id))
        if m and isinstance(v, dict) and v.get("kit_lab"):
            shots.append((float(m.group(1)), m.group(2), v["kit_lab"]))
    return solve(shots, (game.get("leftTeam") or {}).get("jerseyColor"),
                 (game.get("rightTeam") or {}).get("jerseyColor"))

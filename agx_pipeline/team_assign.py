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

Kit colour alone fails when the two kits look alike (both dark: 62/140 on one game) or the
half-time split is weak. With the two rosters, the jersey numbers the tracker read on each
shooter (cv_points.{logId}.who_votes) say the team directly -- a number only one roster has is
that team's. The switch is then scored by both signals (colour counts only as far as the kits
really differ) and the groups are named by the numbers. Seven annotated games, 891 shots:
colour 782 right, numbers + colour 883 (99%).

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


NUM_MIN_SHOTS = 6          # shots with number evidence needed before the numbers are used
NUM_MIN_VOTES = 1.0        # summed read confidence on one-roster-only numbers for a shot to count
COLOUR_WEIGHT = 0.5        # colour's share of the switch score when the kits clearly differ
KIT_DIST_MIN, KIT_DIST_FULL = 15.0, 45.0   # Lab distance between the kits: no weight .. full weight


def _number_evidence(votes: Optional[Dict], left: set, right: set) -> float:
    """+1 = only the left roster's numbers were read on this shooter, -1 = only the right's."""
    if not isinstance(votes, dict):
        return 0.0
    wl = wr = 0.0
    for n, w in votes.items():
        try:
            n, w = str(n), float(w)
        except (TypeError, ValueError):
            continue
        if n in left and n not in right:
            wl += w
        elif n in right and n not in left:
            wr += w
    return (wl - wr) / (wl + wr) if wl + wr >= NUM_MIN_VOTES else 0.0


def _solve_numbers(shots: List[tuple], rosters: Dict) -> Optional[Dict]:
    """The switch + names from jersey numbers (and colour, as far as the kits differ), or None
    when too few shots carry a number only one roster has."""
    left, right = set(map(str, rosters.get("left") or ())), set(map(str, rosters.get("right") or ()))
    if not left or not right:
        return None
    ev = [(s[0], s[1], _number_evidence(s[3] if len(s) > 3 else None, left, right)) for s in shots]
    ev = [e for e in ev if e[2] != 0.0]
    if len(ev) < NUM_MIN_SHOTS:
        return None
    kits = [(s[0], s[1], np.array(s[2])) for s in shots if s[2] is not None]
    times = sorted(s[0] for s in shots)
    cands = [None] + [(times[i - 1] + times[i]) / 2 for i in range(3, len(times) - 3)]
    in_a = lambda side, t, T: (side == "left") == (T is None or t < T)
    den = sum(abs(e) for _, _, e in ev)
    num = np.array([sum(e if in_a(sd, t, T) else -e for t, sd, e in ev) / den for T in cands])
    col = np.array([_sep([k for t, sd, k in kits if in_a(sd, t, T)],
                         [k for t, sd, k in kits if not in_a(sd, t, T)]) for T in cands])
    w = 0.0
    if len(kits) >= MIN_SHOTS and col.max() > 0:
        T = cands[int(col.argmax())]
        dist = float(np.linalg.norm(np.median([k for t, sd, k in kits if in_a(sd, t, T)], axis=0)
                                    - np.median([k for t, sd, k in kits if not in_a(sd, t, T)], axis=0)))
        w = COLOUR_WEIGHT * min(1.0, max(0.0, (dist - KIT_DIST_MIN) / (KIT_DIST_FULL - KIT_DIST_MIN)))
    score = np.abs(num) + (w * col / col.max() if w else 0.0)
    i = int(score.argmax())
    # group A attacks the LEFT basket first; positive evidence = group A is the left team
    return {"switch_epoch": cands[i], "left_basket_first": "left" if num[i] >= 0 else "right",
            "source": "numbers", "number_shots": len(ev), "number_agreement": round(float(abs(num[i])), 2)}


def solve(shots: List[tuple], left_colour: Optional[str], right_colour: Optional[str],
          rosters: Optional[Dict] = None) -> Optional[Dict]:
    """shots: (epoch, basket side 'left'/'right', kit Lab or None[, jersey votes {number: weight}]).
    rosters: {"left": numbers, "right": numbers} of the game's two teams. Returns tracker_teams
    or None."""
    if rosters:
        tt = _solve_numbers(shots, rosters)
        if tt:
            return tt
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
    return {"switch_epoch": T, "left_basket_first": "left" if straight <= swapped else "right",
            "source": "colour"}


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


def assign_game(game: Dict, rosters: Optional[Dict] = None) -> Optional[Dict]:
    """tracker_teams for a Firebase game doc from its cv_points kit colours and, when the two
    rosters are given ({"left": {number: name}, "right": {...}}), the jersey numbers read."""
    shots = []
    for field in ("cv_points", "cv_misses"):        # makes, and misses when the tracker ran on them
        for log_id, v in (game.get(field) or {}).items():
            m = _ID.match(str(log_id))
            if m and isinstance(v, dict) and (v.get("kit_lab") or v.get("who_votes")):
                shots.append((float(m.group(1)), m.group(2), v.get("kit_lab"), v.get("who_votes")))
    return solve(shots, (game.get("leftTeam") or {}).get("jerseyColor"),
                 (game.get("rightTeam") or {}).get("jerseyColor"), rosters)

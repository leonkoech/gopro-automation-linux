"""Which CV highlight clips are dense shooting (warm-ups, half-time, timeouts, post-game), not
game play — kept out of the whole-game reel.

In a game, makes at one basket come a possession or more apart. When warm-ups or a shootaround
run, one basket sees a make every few seconds. A clip is a CORE clip when at least CORE_MIN other
CV clips hit the same basket within ±WINDOW_S of it; a clip within EDGE_S of a core clip at the
same basket is dropped with it (the sparser edges of the same burst).

Measured on 28 annotated games (3067 CV clips, 1347 real in-game makes): drops 58% of the clips
the annotators did not log as game makes, and 0 real makes (0 in every game). It needs only the
clip ids (cv_<epoch>_<side>), so it runs on the finished game with no GT, audio or scorekeeper.
"""
from __future__ import annotations

import os
import re
from typing import Iterable, Set

WINDOW_S = 60.0
CORE_MIN = 6
EDGE_S = 20.0
_ID = re.compile(r"^cv_(\d{9,11}|\d+)_(left|right)$")


def enabled() -> bool:
    return os.getenv("CORE_REEL_DROP_DENSE", "true").lower() in ("1", "true", "yes", "on")


def dense_shooting(keys: Iterable) -> Set[str]:
    """The subset of CV clip ids that fall in a dense-shooting burst at their basket."""
    shots = []
    for k in keys:
        m = _ID.match(str(k)) if k is not None else None
        if m:
            shots.append((float(m.group(1)), m.group(2), str(k)))
    out: Set[str] = set()
    for side in ("left", "right"):
        s = sorted((e, k) for e, sd, k in shots if sd == side)
        ts = [e for e, _ in s]
        core = [sum(1 for u in ts if abs(u - t) <= WINDOW_S) - 1 >= CORE_MIN for t in ts]
        core_ts = [t for t, c in zip(ts, core) if c]
        for t, k in s:
            if any(abs(t - c) <= EDGE_S for c in core_ts):
                out.add(k)
    return out

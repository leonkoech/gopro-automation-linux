"""Cross-side merge + team attribution for CV shot-detection V1.

This package takes the two per-side detection_results.json files from
`Uball_dual_angle_fusion`, merges them into a single ordered timeline,
and emits a list of `cv_shot` log entries with `team = hoop_side`
(per the simplified UBA-214 plan — plays_sync.py downstream maps the
hoop label to leftTeam / rightTeam).

Output: a list of `cv_shot` log entries that are appended to the Firebase
basketball-games document's `logs` array. From there, the existing
`plays_sync.create_plays_from_firebase_logs` pipeline turns them into
annotation-tool cards.
"""

from .merge import MergedShot, merge
from .team_attribution import (
    HOOP_SIDE_LEFT,
    HOOP_SIDE_RIGHT,
    SIDE_TO_HOOP,
    TEAM1_FB_LABEL,
    TEAM2_FB_LABEL,
    attribute_team,            # legacy / not used by V1 merge path
    find_halftime_seconds,     # legacy / not used by V1 merge path
    hoop_side_for_shot,
)
from .firebase_emitter import (
    build_cv_shot_log,
    emit_cv_logs,
    is_cv_already_emitted,
)

__all__ = [
    "MergedShot",
    "merge",
    "HOOP_SIDE_LEFT",
    "HOOP_SIDE_RIGHT",
    "SIDE_TO_HOOP",
    "TEAM1_FB_LABEL",
    "TEAM2_FB_LABEL",
    "attribute_team",
    "find_halftime_seconds",
    "hoop_side_for_shot",
    "build_cv_shot_log",
    "emit_cv_logs",
    "is_cv_already_emitted",
]

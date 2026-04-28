"""Cross-side merge + team attribution for CV shot-detection V1.

This package takes the two per-side detection_results.json files from
`Uball_dual_angle_fusion`, merges them into a single ordered timeline, and
attributes each shot to a team using the game's `startingSideTeam1` field
plus the halftime flip (detected from `period_changed` logs).

Output: a list of `cv_shot` log entries that are appended to the Firebase
basketball-games document's `logs` array. From there, the existing
`plays_sync.create_plays_from_firebase_logs` pipeline turns them into
annotation-tool cards.
"""

from .merge import MergedShot, merge
from .team_attribution import (
    SIDE_TO_HOOP,
    TEAM1_FB_LABEL,
    TEAM2_FB_LABEL,
    attribute_team,
    find_halftime_seconds,
)
from .firebase_emitter import (
    build_cv_shot_log,
    emit_cv_logs,
    is_cv_already_emitted,
)

__all__ = [
    "MergedShot",
    "merge",
    "SIDE_TO_HOOP",
    "TEAM1_FB_LABEL",
    "TEAM2_FB_LABEL",
    "attribute_team",
    "find_halftime_seconds",
    "build_cv_shot_log",
    "emit_cv_logs",
    "is_cv_already_emitted",
]

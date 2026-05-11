"""Unit tests for cv_merge.team_attribution.

Covers:
  * `hoop_side_for_shot` — the V1 simplified attribution (UBA-214 update)
    that the merge container actually uses.
  * `attribute_team` — kept under test only because the function is still
    exported for back-compat callers. NOT the V1 path.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from cv_merge.team_attribution import (  # noqa: E402
    HOOP_SIDE_LEFT,
    HOOP_SIDE_RIGHT,
    attribute_team,
    find_halftime_seconds,
    hoop_side_for_shot,
)


# ============================================================================
# hoop_side_for_shot — V1 attribution path
# ============================================================================
class TestHoopSideForShot:
    def test_explicit_left_from_shot_source_wins(self):
        # Even if SIDE=A would imply "right", an explicit per-shot
        # hoop_side overrides (future-proofing for a per-shot detector).
        result = hoop_side_for_shot(
            side="A",
            shot_source={"hoop_side": "left", "timestamp_seconds": 12.0},
        )
        assert result == "left"

    def test_explicit_right_from_shot_source_wins(self):
        result = hoop_side_for_shot(
            side="B",
            shot_source={"hoop_side": "right", "timestamp_seconds": 34.0},
        )
        assert result == "right"

    def test_side_A_falls_back_to_right(self):
        # No hoop_side in payload — fall back to side-derived.
        result = hoop_side_for_shot(
            side="A",
            shot_source={"timestamp_seconds": 12.0},
        )
        assert result == "right"

    def test_side_B_falls_back_to_left(self):
        result = hoop_side_for_shot(
            side="B",
            shot_source={"timestamp_seconds": 34.0},
        )
        assert result == "left"

    def test_no_shot_source_falls_back_to_side(self):
        assert hoop_side_for_shot(side="A") == "right"
        assert hoop_side_for_shot(side="B") == "left"

    def test_invalid_hoop_side_in_source_falls_back(self):
        """A garbage hoop_side value in the payload falls through to the
        side-derived default rather than propagating bad data."""
        result = hoop_side_for_shot(
            side="A",
            shot_source={"hoop_side": "middle"},
        )
        assert result == "right"

    def test_unknown_side_raises(self):
        with pytest.raises(ValueError, match="unknown side"):
            hoop_side_for_shot(side="X")

    def test_constants_match_v1_convention(self):
        # plays_sync.py maps team="left" → leftTeam, team="right" → rightTeam.
        # Keep the constants in lock-step with that convention.
        assert HOOP_SIDE_LEFT == "left"
        assert HOOP_SIDE_RIGHT == "right"


# ============================================================================
# attribute_team — legacy halftime-aware path (NOT used in V1 merge)
# ============================================================================
class TestAttributeTeamLegacy:
    def test_first_half_team1_attacks_their_starting_hoop(self):
        # team1 attacks left at tip-off; Side B watches left hoop;
        # so a Side B shot in the first half is team1 → "left".
        assert attribute_team(
            side="B", timestamp_seconds=5.0,
            starting_side_team1="left", halftime_ts=None,
        ) == "left"

    def test_first_half_team2_attacks_opposite_hoop(self):
        # team1 attacks left; team2 attacks right; Side A watches right.
        # Side A first-half shot = team2 → "right".
        assert attribute_team(
            side="A", timestamp_seconds=5.0,
            starting_side_team1="left", halftime_ts=None,
        ) == "right"

    def test_second_half_flip(self):
        # team1 started attacking left → after halftime attacks right.
        # Side A watches right → second-half Side A shot = team1 → "left".
        assert attribute_team(
            side="A", timestamp_seconds=2400.0,
            starting_side_team1="left", halftime_ts=1200.0,
        ) == "left"

    def test_missing_starting_side_defaults_to_left(self):
        # No starting_side_team1 → fall back to "team1 attacks left".
        assert attribute_team(
            side="B", timestamp_seconds=5.0,
            starting_side_team1=None, halftime_ts=None,
        ) == "left"

    def test_unknown_side_raises(self):
        with pytest.raises(ValueError):
            attribute_team(
                side="C", timestamp_seconds=1.0,
                starting_side_team1="left", halftime_ts=None,
            )


# ============================================================================
# find_halftime_seconds — unchanged from PR #32, kept under regression
# ============================================================================
class TestFindHalftime:
    def test_returns_offset_when_period_changed_log_present(self):
        game = {
            "createdAt": "2026-04-15T18:00:00Z",
            "logs": [
                {"actionType": "tipoff",         "timestamp": "2026-04-15T18:00:30Z"},
                {"actionType": "period_changed", "timestamp": "2026-04-15T18:20:00Z",
                 "payload": {"newValue": "2nd"}},
            ],
        }
        assert find_halftime_seconds(game) == pytest.approx(1200.0)

    def test_returns_none_when_no_period_changed_log(self):
        game = {
            "createdAt": "2026-04-15T18:00:00Z",
            "logs": [{"actionType": "tipoff",
                      "timestamp": "2026-04-15T18:00:30Z"}],
        }
        assert find_halftime_seconds(game) is None

    def test_returns_none_when_createdAt_missing(self):
        assert find_halftime_seconds({"logs": []}) is None

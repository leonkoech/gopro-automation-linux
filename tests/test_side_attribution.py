"""Unit tests for side_attribution — the halftime-aware scoring-side mapping."""

from agx_pipeline.side_attribution import (
    HOOP_CAMERAS,
    scoring_hoop_side,
    team1_attacking_side,
)


def test_user_example_first_half():
    # "team 1 is scoring on left so FL and NL will record that"
    # team1 = left button, H1, team1 starts attacking the left hoop.
    assert scoring_hoop_side("left", "1st", "left") == "left"
    assert HOOP_CAMERAS["left"] == ("FL", "NL")


def test_user_example_second_half_flips():
    # "when halftime it switches team 1 now is scoring on right FR and NR"
    assert scoring_hoop_side("left", "2nd", "left") == "right"
    assert HOOP_CAMERAS["right"] == ("FR", "NR")


def test_team2_is_opposite_of_team1():
    assert scoring_hoop_side("right", "1st", "left") == "right"
    assert scoring_hoop_side("right", "2nd", "left") == "left"


def test_starting_side_right():
    assert scoring_hoop_side("left", "1st", "right") == "right"
    assert scoring_hoop_side("left", "2nd", "right") == "left"
    assert scoring_hoop_side("right", "1st", "right") == "left"


def test_unknown_starting_side_defaults_left():
    assert scoring_hoop_side("left", "1st", None) == "left"
    assert scoring_hoop_side("left", "1st", "") == "left"
    assert scoring_hoop_side("left", "1st", "bogus") == "left"


def test_unknown_team_returns_none_for_fallback():
    assert scoring_hoop_side(None, "1st", "left") is None
    assert scoring_hoop_side("middle", "1st", "left") is None


def test_team1_attacking_side():
    assert team1_attacking_side("1st", "left") == "left"
    assert team1_attacking_side("2nd", "left") == "right"
    assert team1_attacking_side("1st", "right") == "right"
    assert team1_attacking_side("2nd", "right") == "left"
    assert team1_attacking_side(None, None) == "left"

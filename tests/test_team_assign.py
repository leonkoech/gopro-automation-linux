"""Team from basket + half: every shot (make or miss) gets the team attacking that basket, with
one half-time swap found from the shooters' kit colours."""
import numpy as np
import pytest

from agx_pipeline.team_assign import solve, team_for_shot

RED, BLUE = [60.0, 175.0, 150.0], [40.0, 140.0, 100.0]


def _shots(switch=1000.0, left_first_colour=RED, other=BLUE, n=20):
    rng = np.random.default_rng(0)
    out = []
    for i in range(n):
        t = 100.0 + i * 100.0
        side = "left" if i % 2 else "right"
        before = t < switch
        colour = left_first_colour if (side == "left") == before else other
        out.append((t, side, list(np.array(colour) + rng.normal(0, 3, 3))))
    return out


@pytest.mark.unit
def test_finds_half_time_and_names_teams():
    tt = solve(_shots(), "#ef4444", "#3b82f6")          # left team registered red
    assert tt["left_basket_first"] == "left"
    assert 900 <= tt["switch_epoch"] <= 1100
    assert team_for_shot(tt, "left", 200) == "left"      # red attacks the left basket first
    assert team_for_shot(tt, "right", 200) == "right"
    assert team_for_shot(tt, "left", 1500) == "right"    # swapped after half-time
    assert team_for_shot(tt, "right", 1500) == "left"


@pytest.mark.unit
def test_registered_colours_swap_the_names_not_the_structure():
    tt = solve(_shots(), "#3b82f6", "#ef4444")          # left team registered blue
    assert tt["left_basket_first"] == "right"
    assert team_for_shot(tt, "left", 200) == "right"


@pytest.mark.unit
def test_too_few_shots_or_no_colours_gives_nothing():
    assert solve(_shots(n=4), "#ef4444", "#3b82f6") is None
    assert solve(_shots(), None, "#3b82f6") is None
    assert team_for_shot(None, "left", 100) is None


# ---- team from jersey numbers: the kits can look the same, the rosters do not ------------- #
# 2026-10-12, seven annotated games: kit colour named the team right on 782/891 shots (both
# teams in dark kits on one game: 62/140); numbers + colour together 883/891.
ROSTERS = {"left": {"1": "A", "5": "B", "7": "C"}, "right": {"2": "D", "9": "E", "7": "F"}}
DARK = [18.0, 140.0, 122.0]


def _numbered(switch=1000.0, n=20, kit_a=DARK, kit_b=DARK):
    """Left team (numbers 1/5) attacks the left basket before `switch`; kits as given."""
    rng = np.random.default_rng(1)
    out = []
    for i in range(n):
        t = 100.0 + i * 100.0
        side = "left" if i % 2 else "right"
        is_left_team = (side == "left") == (t < switch)
        votes = {("1" if i % 4 < 2 else "5") if is_left_team else ("2" if i % 4 < 2 else "9"): 6.0, "7": 2.0}
        kit = kit_a if is_left_team else kit_b
        out.append((t, side, list(np.array(kit) + rng.normal(0, 3, 3)), votes))
    return out


@pytest.mark.unit
def test_same_coloured_kits_are_split_by_the_numbers():
    tt = solve(_numbered(), "#f97316", "#ec4899", rosters=ROSTERS)
    assert tt["left_basket_first"] == "left" and 900 <= tt["switch_epoch"] <= 1100
    assert tt["source"] == "numbers"
    assert team_for_shot(tt, "left", 200) == "left" and team_for_shot(tt, "left", 1500) == "right"


@pytest.mark.unit
def test_numbers_name_the_teams_even_when_the_registered_colours_are_swapped():
    tt = solve(_numbered(kit_a=RED, kit_b=BLUE), "#3b82f6", "#ef4444", rosters=ROSTERS)   # registered wrong way round
    assert tt["left_basket_first"] == "left"


@pytest.mark.unit
def test_a_number_both_teams_wear_is_no_evidence():
    shots = [(t, s, k, {"7": 9.0}) for t, s, k, _ in _numbered(kit_a=RED, kit_b=BLUE)]
    tt = solve(shots, "#ef4444", "#3b82f6", rosters=ROSTERS)
    assert tt["source"] == "colour" and tt["left_basket_first"] == "left"


@pytest.mark.unit
def test_without_rosters_or_votes_the_colour_rule_is_unchanged():
    assert solve(_shots(), "#ef4444", "#3b82f6", rosters=ROSTERS) == dict(solve(_shots(), "#ef4444", "#3b82f6"))
    assert solve(_numbered(kit_a=RED, kit_b=BLUE), "#ef4444", "#3b82f6")["source"] == "colour"


@pytest.mark.unit
def test_assign_game_reads_votes_and_rosters():
    from agx_pipeline.team_assign import assign_game
    cv = {"cv_%d_%s" % (1700000000 + int(t), s): {"kit_lab": k, "who_votes": v} for t, s, k, v in _numbered()}
    tt = assign_game({"cv_points": cv, "leftTeam": {"jerseyColor": "#f97316"}, "rightTeam": {"jerseyColor": "#ec4899"}}, ROSTERS)
    assert tt["source"] == "numbers" and tt["left_basket_first"] == "left"

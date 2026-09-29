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

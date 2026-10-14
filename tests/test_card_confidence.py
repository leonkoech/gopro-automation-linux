"""Measured per-field confidence on a CV card and the flag the weakest field sets."""
import pytest

from agx_pipeline.card_confidence import card_confidence, flag_from_confidence

TEAMS = {"source": "numbers", "number_agreement": 0.8}


def v(**kw):
    base = {"zone": "2PT", "line_px": 80.0, "who_votes": {"7": 9.0, "2": 1.0}, "who_from": "clip"}
    base.update(kw)
    return base


@pytest.mark.unit
def test_a_clear_card_is_green_with_every_field_measured():
    c = card_confidence(v(), ("7", "Sam"), True, TEAMS)
    assert c == {"makemiss": 0.98, "type": 0.93, "team": 0.95, "shooter": 0.9, "card": pytest.approx(0.78, abs=0.01)}
    assert flag_from_confidence(c)[0] == "green"


@pytest.mark.unit
def test_feet_near_a_line_sets_yellow_and_says_why():
    f, why = flag_from_confidence(card_confidence(v(line_px=-12.0), ("7", "Sam"), True, TEAMS))
    assert f == "yellow" and "shot type" in why and "72%" in why


@pytest.mark.unit
def test_free_throws_need_no_line_margin():
    assert card_confidence(v(zone="FREE_THROW", line_px=3.0), ("7", "Sam"), True, TEAMS)["type"] == 0.95


@pytest.mark.unit
def test_shooter_sources_rank_as_measured():
    s = lambda **kw: card_confidence(v(**kw), ("7", "Sam"), True, TEAMS)["shooter"]
    assert s() == 0.9 > s(who_votes={"7": 4.0, "2": 2.0}) == 0.72 > s(who_from="timeline") == 0.51
    looks = lambda c: card_confidence(v(who_looks={"n": "7", "conf": c}), ("7", "Sam", "looks"), True, TEAMS)["shooter"]
    assert looks(0.9) == 0.83 and looks(0.7) == 0.64 and looks(0.5) == 0.0


@pytest.mark.unit
def test_team_confidence_follows_how_the_team_was_called():
    t = lambda tt: card_confidence(v(), ("7", "Sam"), True, tt)["team"]
    assert t(TEAMS) == 0.95 and t({"source": "numbers", "number_agreement": 0.4}) == 0.80
    assert t({"source": "colour"}) == t({"switch_epoch": 1.0}) == 0.83 and t(None) == 0.0


@pytest.mark.unit
def test_a_missing_answer_is_red_and_named():
    assert flag_from_confidence(card_confidence(v(zone=None), ("7", "Sam"), True, TEAMS)) == ("red", "no shot type")
    assert flag_from_confidence(card_confidence(v(), None, False, TEAMS)) == ("red", "no shooter")


@pytest.mark.unit
def test_paused_clock_and_bursts_stay_red_first():
    c = card_confidence(v(), ("7", "Sam"), True, TEAMS)
    assert flag_from_confidence(c, paused=True)[0] == "red" and "paused" in flag_from_confidence(c, paused=True)[1]
    assert "warm-up" in flag_from_confidence(c, burst=True)[1]

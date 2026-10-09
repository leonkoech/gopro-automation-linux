"""Green / yellow / red on CV annotation cards."""
from agx_pipeline.card_flag import card_flag, CONFIDENCE


def v(zone="2PT", line_px=80.0, who_from="clip"):
    return {"zone": zone, "line_px": line_px, "who_from": who_from}


def test_green_needs_a_clear_zone_and_a_jersey_from_the_clip():
    assert card_flag(v(), ("7", "Sam"))[0] == "green"


def test_feet_near_a_line_is_yellow_unless_it_is_a_free_throw():
    assert card_flag(v(line_px=-12.0), ("7", "Sam"))[0] == "yellow"
    assert card_flag(v(zone="FREE_THROW", line_px=5.0), ("7", "Sam"))[0] == "green"


def test_jersey_only_from_following_back_is_yellow():
    f, why = card_flag(v(who_from="timeline"), ("7", "Sam"))
    assert f == "yellow" and "followed back" in why


def test_missing_type_or_jersey_is_red():
    assert card_flag(v(zone=None), ("7", "Sam"))[0] == "red"
    assert card_flag(v(), None)[0] == "red"
    assert card_flag(None, None)[0] == "red"


def test_shot_on_a_paused_clock_is_red():
    f, why = card_flag(v(), ("7", "Sam"), paused=True)
    assert f == "red" and "paused" in why


def test_confidence_bands_keep_the_old_badge_consistent():
    assert CONFIDENCE["green"] >= 0.7 > CONFIDENCE["yellow"] > CONFIDENCE["red"]

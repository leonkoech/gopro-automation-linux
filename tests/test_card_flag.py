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


def test_a_burst_or_a_paused_clock_is_red_first():
    from agx_pipeline.card_flag import card_flag as cf
    assert cf(v(), ("7", "Sam"), burst=True)[0] == "red"
    assert "warm-up" in cf(v(), ("7", "Sam"), burst=True)[1]


def test_paused_intervals_from_the_scorekeeper_timer():
    from agx_pipeline.card_flag import paused_intervals, is_paused
    logs = [{"actionType": "game_started", "timestamp": "2026-08-20T00:00:00Z"},
            {"actionType": "timer_started", "timestamp": "2026-08-20T00:05:00Z"},
            {"actionType": "timer_paused", "timestamp": "2026-08-20T00:20:00Z"},
            {"actionType": "timer_started", "timestamp": "2026-08-20T00:22:00Z"},
            {"actionType": "game_ended", "timestamp": "2026-08-20T01:00:00Z"}]
    iv = paused_intervals(logs)
    from datetime import datetime, timezone
    t = lambda m: datetime(2026, 8, 20, 0, m, tzinfo=timezone.utc).timestamp()
    assert is_paused(iv, t(2)) and is_paused(iv, t(21)) and not is_paused(iv, t(10))
    assert paused_intervals([{"actionType": "game_started", "timestamp": "2026-08-20T00:00:00Z"}]) is None


def test_burst_shots_marks_a_warmup_but_not_game_pace():
    from agx_pipeline.card_flag import burst_shots
    warm = ["cv_%d_left" % (1000 + 4 * i) for i in range(15)]          # a shot every 4 s
    game = ["cv_%d_left" % (5000 + 40 * i) for i in range(10)]         # a shot every 40 s
    out = burst_shots(warm + game)
    assert set(warm) <= out and not (set(game) & out)

"""Dense-shooting filter: warm-ups, half-time and post-game shootarounds out of the reel."""
from agx_pipeline.deadball import dense_shooting


def ids(side, epochs):
    return ["cv_%d_%s" % (e, side) for e in epochs]


def test_live_play_pace_is_kept():
    # one make every ~40 s per basket, alternating: normal game pace
    live = ids("left", range(1000, 3000, 80)) + ids("right", range(1040, 3000, 80))
    assert dense_shooting(live) == set()


def test_warmup_burst_and_its_edges_are_dropped():
    warm = ids("left", range(0, 120, 8))                  # 15 makes in 2 minutes at one basket
    edge = ids("left", [130])                              # a straggler 10 s after the burst
    later = ids("left", [400])                             # a real make long after
    out = dense_shooting(warm + edge + later)
    assert set(warm) <= out and "cv_130_left" in out
    assert "cv_400_left" not in out


def test_density_is_per_basket():
    warm = ids("left", range(0, 120, 8))
    other = ids("right", [60])                             # a single make at the other end
    assert "cv_60_right" not in dense_shooting(warm + other)


def test_non_cv_keys_are_ignored():
    assert dense_shooting(["abc", "cv_x_left", None]) == set()

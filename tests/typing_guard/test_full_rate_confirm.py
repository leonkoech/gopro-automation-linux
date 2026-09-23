"""The full-rate confirm pass: keep the right segments, time the shots right,
and never double-count a shot the live loop already has.

Background: the live loop feeds the ball model every 4th frame, and a clean
swish leaves too few samples to pass logic.py's full-rate gates. On identical
frames a full-rate re-read of 16% of segments took made shots 29 -> 37.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from agx_pipeline.shot_detect import confirm, live  # noqa: E402
from agx_pipeline.shot_detect.live import LiveShotScorer  # noqa: E402


class _G:
    """Rim at the origin with radius 1: rho is squared distance in rim radii."""
    def rho(self, x, y):
        return x * x + y * y


def _scorer():
    s = LiveShotScorer.__new__(LiveShotScorer)
    s._confirm_keep, s._seg_reals = {}, {}
    return s


def _iso(e):
    return datetime.fromtimestamp(e, timezone.utc).isoformat()


# ---- which segments the live loop keeps --------------------------------- #
@pytest.mark.unit
def test_a_gated_window_with_the_ball_at_the_rim_is_kept(monkeypatch):
    monkeypatch.setattr(live, "CONFIRM_ENABLED", True)
    s = _scorer()
    near = [(0, 0.5, 0.5, 10.0, 0.9)]                  # rho 0.5 rim radii
    s._note_confirm("SR", 7, "/x/seg_00007_SR.mp4", _G(), near,
                    [{"skipped": "gate (evidence 3, over_rim True)"}], 4.5)
    assert s._confirm_keep == {"SR": {7: "/x/seg_00007_SR.mp4"}}


@pytest.mark.unit
def test_a_window_that_already_decided_is_not_kept(monkeypatch):
    """It produced a verdict at stride 4 -- a second look buys nothing."""
    monkeypatch.setattr(live, "CONFIRM_ENABLED", True)
    s = _scorer()
    s._note_confirm("SR", 7, "p", _G(), [(0, 0.1, 0.1, 10.0, 0.9)],
                    [{"verdict": "MAKE", "t": 1.0}], 4.5)
    assert s._confirm_keep == {}


@pytest.mark.unit
def test_a_gated_window_with_the_ball_nowhere_near_the_rim_is_not_kept(monkeypatch):
    """Keeps disk and ingest time proportional to real rim activity."""
    monkeypatch.setattr(live, "CONFIRM_ENABLED", True)
    s = _scorer()
    s._note_confirm("SL", 3, "p", _G(), [(0, 5.0, 5.0, 10.0, 0.9)],
                    [{"skipped": "gate"}], 4.5)
    assert s._confirm_keep == {}


@pytest.mark.unit
def test_nothing_is_kept_when_the_feature_is_off(monkeypatch):
    monkeypatch.setattr(live, "CONFIRM_ENABLED", False)
    s = _scorer()
    s._note_confirm("SL", 3, "p", _G(), [(0, 0.1, 0.1, 10.0, 0.9)],
                    [{"skipped": "gate"}], 4.5)
    assert s._confirm_keep == {}


@pytest.mark.unit
def test_segment_durations_are_recorded_even_when_nothing_is_kept(monkeypatch):
    """The confirm pass needs them to place a shot on the wall clock."""
    monkeypatch.setattr(live, "CONFIRM_ENABLED", False)
    s = _scorer()
    s._note_confirm("SL", 3, "p", _G(), [], [], 4.53)
    assert s._seg_reals == {"SL": [4.53]}


@pytest.mark.unit
def test_advancing_the_window_never_deletes_a_candidate(tmp_path):
    s = _scorer()
    cand = tmp_path / "seg_00005_SL.mp4"
    cand.write_bytes(b"x")
    s._confirm_keep = {"SL": {5: str(cand)}}
    prev = {"SL": (5, str(cand))}
    s._advance_prev(prev, "SL", 6, str(tmp_path / "seg_00006_SL.mp4"))
    assert cand.exists()


@pytest.mark.unit
def test_advancing_the_window_still_deletes_an_ordinary_segment(tmp_path):
    s = _scorer()
    old = tmp_path / "seg_00005_SL.mp4"
    old.write_bytes(b"x")
    prev = {"SL": (5, str(old))}
    s._advance_prev(prev, "SL", 6, str(tmp_path / "seg_00006_SL.mp4"))
    assert not old.exists()


@pytest.mark.unit
def test_the_manifest_carries_what_the_confirm_pass_needs(tmp_path):
    s = _scorer()
    s._seg_reals = {"SR": [4.4, 4.5, 4.6]}
    keep = tmp_path / "seg_00009_SR.mp4"
    keep.write_bytes(b"x")
    (tmp_path / "seg_00008_SR.mp4").write_bytes(b"x")          # processed -> pruned
    s._keep_unscanned(str(tmp_path), [], "g1",
                      confirm=[(9, "SR", str(keep))], fps=119.9)
    man = json.loads((tmp_path / "UNSCANNED.json").read_text())
    assert man["confirm"] == ["seg_00009_SR.mp4"] and man["unscanned"] == []
    assert man["fps"] == 119.9
    assert man["seg_real"] == {"SR": 4.5}
    assert "seg_00009_SR.mp4" in man["close"]
    assert keep.exists() and not (tmp_path / "seg_00008_SR.mp4").exists()


# ---- placing a recovered shot on the wall clock --------------------------- #
@pytest.mark.unit
def test_wallclock_uses_the_measured_segment_duration():
    """A 4s media segment that really spanned 4.5s: a shot 2.0 media-seconds in
    happened 2.25 wall-seconds after the segment started."""
    close = 1000.0
    assert confirm.window_wallclock(close, 4.5, 2.0, 4.0) == pytest.approx(997.75)


@pytest.mark.unit
def test_wallclock_falls_back_to_the_nominal_duration():
    assert confirm.window_wallclock(1000.0, None, 2.0, 4.0) == pytest.approx(998.0)


# ---- never double-count what live already has ----------------------------- #
@pytest.mark.unit
def test_a_shot_live_already_has_on_that_side_is_dropped():
    have = [{"side": "left", "wallclock": _iso(100.0)}]
    got = confirm.dedup_new([{"side": "left", "wallclock": _iso(101.5)}], have, 2.5)
    assert got == []


@pytest.mark.unit
def test_the_same_moment_on_the_other_hoop_is_a_different_shot():
    have = [{"side": "left", "wallclock": _iso(100.0)}]
    got = confirm.dedup_new([{"side": "right", "wallclock": _iso(100.0)}], have, 2.5)
    assert len(got) == 1


@pytest.mark.unit
def test_confirmed_shots_do_not_double_count_each_other():
    found = [{"side": "left", "wallclock": _iso(100.0)},
             {"side": "left", "wallclock": _iso(101.0)},
             {"side": "left", "wallclock": _iso(110.0)}]
    got = confirm.dedup_new(found, [], 2.5)
    assert [datetime.fromisoformat(s["wallclock"]).timestamp() for s in got] == [100.0, 110.0]


@pytest.mark.unit
def test_a_shot_with_an_unreadable_wallclock_is_skipped_not_raised():
    assert confirm.dedup_new([{"side": "left", "wallclock": "nope"}], [], 2.5) == []


@pytest.mark.unit
def test_no_manifest_means_nothing_to_do(tmp_path):
    assert confirm.read_manifest(str(tmp_path)) is None
    (tmp_path / "UNSCANNED.json").write_text("{not json")
    assert confirm.read_manifest(str(tmp_path)) is None

"""Watchdog stall gaps must survive the concat, and a short angle must alarm.

The bug these lock down: `_concat_segments` used to butt restart segments
end-to-end with `ffmpeg -f concat -c copy`, which DELETES the wall-clock stall
window between them. One camera restarting mid-game therefore came out ~20s
shorter than the other four and slid progressively out of sync with them —
shipped unnoticed in the 2026-08-17 and 2026-08-19 games until the annotators
reported it by hand.
"""

import os

import pytest

from agx_pipeline import recording
from agx_pipeline.recording import (
    ANGLE_SKEW_ALARM_SEC,
    MAX_STALL_GAP_SEC,
    MIN_STALL_GAP_SEC,
    _check_angle_skew,
)


class FakeController:
    """Just enough of RecordingController to exercise the gap arithmetic."""
    _stall_gaps = recording.RecordingController._stall_gaps


@pytest.fixture
def ctl():
    return FakeController()


def _run(segments, seg_starts):
    return {"segments": segments, "seg_starts": seg_starts, "angle": "FR"}


# ---- gap arithmetic ------------------------------------------------------- #

def test_no_restart_means_no_gap(ctl):
    run = _run(["a.mp4"], [1000.0])
    assert ctl._stall_gaps(run, ["a.mp4"], [600.0]) == [0.0]


def test_the_stall_window_is_measured_not_dropped(ctl):
    # seg A starts at t=1000 and holds 600s of video -> it really ended at 1600.
    # The watchdog got seg B going at 1620.5 => 20.5s of the game was never filmed.
    run = _run(["a.mp4", "b.mp4"], [1000.0, 1620.5])
    gaps = ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [600.0, 900.0])
    assert gaps[0] == 0.0
    assert gaps[1] == pytest.approx(20.5)


def test_gap_reproduces_the_reported_20_second_loss(ctl):
    # The measured symptom: FR 20.5s shorter than FL/NL/SL/SR on 2026-08-19.
    run = _run(["a.mp4", "b.mp4"], [0.0, 1490.5])
    gaps = ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [1470.0, 1438.5])
    assert sum(gaps) == pytest.approx(20.5)
    # ... and putting it back makes the angle whole again.
    assert 1470.0 + sum(gaps) + 1438.5 == pytest.approx(2929.0)


def test_multiple_restarts_accumulate(ctl):
    run = _run(["a.mp4", "b.mp4", "c.mp4"], [0.0, 110.0, 230.0])
    gaps = ctl._stall_gaps(run, ["a.mp4", "b.mp4", "c.mp4"], [100.0, 100.0, 50.0])
    assert gaps == [0.0, pytest.approx(10.0), pytest.approx(20.0)]


def test_subframe_jitter_is_not_a_gap(ctl):
    run = _run(["a.mp4", "b.mp4"], [0.0, 100.05])
    assert ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [100.0, 50.0])[1] == 0.0
    assert MIN_STALL_GAP_SEC > 0.05


def test_a_clock_glitch_never_punches_an_absurd_hole(ctl):
    run = _run(["a.mp4", "b.mp4"], [0.0, MAX_STALL_GAP_SEC + 10_000])
    assert ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [100.0, 50.0])[1] == 0.0


def test_negative_gap_is_ignored(ctl):
    # Overlapping stamps (clock stepped back) must not shorten the timeline.
    run = _run(["a.mp4", "b.mp4"], [0.0, 50.0])
    assert ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [100.0, 50.0])[1] == 0.0


def test_unprobeable_segment_contributes_no_gap(ctl):
    run = _run(["a.mp4", "b.mp4"], [0.0, 200.0])
    assert ctl._stall_gaps(run, ["a.mp4", "b.mp4"], [None, 50.0])[1] == 0.0


def test_missing_stamp_falls_back_to_file_mtime(ctl, tmp_path, monkeypatch):
    # Runs recovered from the pid journal after a service restart have no
    # seg_starts; mtime - duration still recovers when a segment began.
    a, b = tmp_path / "a.mp4", tmp_path / "b.mp4"
    a.write_bytes(b"x")
    b.write_bytes(b"x")
    os.utime(a, (0, 1600.0))          # A finalized at 1600
    os.utime(b, (0, 2520.5))          # B finalized at 2520.5, holding 900s => began 1620.5
    run = {"segments": [], "seg_starts": [], "angle": "FR"}
    gaps = ctl._stall_gaps(run, [str(a), str(b)], [600.0, 900.0])
    assert gaps[1] == pytest.approx(20.5)


def test_stamps_win_over_mtime_when_both_exist(ctl, tmp_path):
    a, b = tmp_path / "a.mp4", tmp_path / "b.mp4"
    a.write_bytes(b"x")
    b.write_bytes(b"x")
    os.utime(a, (0, 9_999_999.0))     # misleading mtime
    os.utime(b, (0, 9_999_999.0))
    run = _run([str(a), str(b)], [1000.0, 1620.5])
    assert ctl._stall_gaps(run, [str(a), str(b)], [600.0, 900.0])[1] == pytest.approx(20.5)


# ---- the alarm ------------------------------------------------------------ #

def _res(**durs):
    return [{"angle": a, "duration": d, "ok": True} for a, d in durs.items()]


def test_healthy_angles_report_a_small_skew(caplog):
    skew = _check_angle_skew(_res(FL=2929.03, FR=2929.10, NL=2929.09))
    assert skew == pytest.approx(0.07, abs=0.01)
    assert "ANGLE SKEW" not in caplog.text


def test_the_20_second_loss_raises_the_alarm(caplog):
    # The real 2026-08-19 numbers.
    skew = _check_angle_skew(_res(FL=2929.03, FR=2908.53, NL=2929.09))
    assert skew == pytest.approx(20.56, abs=0.01)   # NL (longest) - FR (shortest)
    assert "ANGLE SKEW" in caplog.text
    assert "FR is short" in caplog.text


def test_alarm_threshold_is_seconds_not_minutes():
    assert 0 < ANGLE_SKEW_ALARM_SEC <= 5


def test_shot_cameras_do_not_count_toward_skew():
    # SL/SR are a different capture path at a different frame rate; they are a
    # useful control but must not drive the tracking-angle alarm.
    skew = _check_angle_skew([{"angle": "FL", "duration": 2929.0, "ok": True},
                              {"angle": "SL", "duration": 100.0, "ok": True}])
    assert skew is None


def test_failed_angles_are_excluded():
    skew = _check_angle_skew([{"angle": "FL", "duration": 2929.0, "ok": True},
                              {"angle": "FR", "duration": None, "ok": False}])
    assert skew is None


def test_one_angle_alone_is_not_comparable():
    assert _check_angle_skew(_res(FL=2929.0)) is None
    assert _check_angle_skew([]) is None

"""The service's only job for the tracker is to drop a job file; it must be atomic, complete,
and off unless SHOT_TRACKER_QUEUE is set."""
import json
import os

import pytest

from agx_pipeline.tracker_queue import enqueue_tracker_job, tracker_queue_enabled


@pytest.mark.unit
def test_off_by_default(monkeypatch):
    monkeypatch.delenv("SHOT_TRACKER_QUEUE", raising=False)
    assert tracker_queue_enabled() is False
    monkeypatch.setenv("SHOT_TRACKER_QUEUE", "true")
    assert tracker_queue_enabled() is True


@pytest.mark.unit
def test_job_file_is_complete_and_atomic(tmp_path):
    p = enqueue_tracker_job("G1", "cv_17_left", "FL", "/clips/cv_17_left_FL.mp4", 5.0,
                            "highlights/2026-10-01/G1/cv_17_left_FL.mp4", True, queue_dir=str(tmp_path))
    assert p and os.path.basename(p) == "G1__cv_17_left.json"
    job = json.load(open(p))
    assert job["angle"] == "FL" and job["pre"] == 5.0 and job["made"] is True
    assert job["s3_key"].endswith("_FL.mp4")
    assert not [f for f in os.listdir(tmp_path / "pending") if f.endswith(".tmp")]


@pytest.mark.unit
def test_failure_never_raises(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    assert enqueue_tracker_job("G", "l", "FL", "c", 5, None, None, queue_dir=str(blocker)) is None


# ---- missed shots to the tracker (SHOT_TRACK_MISSES) ------------------------------------- #
@pytest.mark.unit
def test_misses_are_off_by_default_and_need_the_queue(monkeypatch):
    from agx_pipeline.tracker_queue import track_misses_enabled
    monkeypatch.delenv("SHOT_TRACK_MISSES", raising=False)
    monkeypatch.setenv("SHOT_TRACKER_QUEUE", "true")
    assert track_misses_enabled() is False
    monkeypatch.setenv("SHOT_TRACK_MISSES", "true")
    assert track_misses_enabled() is True
    monkeypatch.setenv("SHOT_TRACKER_QUEUE", "false")
    assert track_misses_enabled() is False


@pytest.mark.unit
def test_a_miss_job_is_raw_and_never_overwrites_the_make_with_the_same_id(tmp_path):
    make = enqueue_tracker_job("G1", "cv_17_left", "FL", "/c/make.mp4", 5.0, "k", True, queue_dir=str(tmp_path))
    miss = enqueue_tracker_job("G1", "cv_17_left", "FL", "/c/raw.mp4", 5.0, None, False, queue_dir=str(tmp_path),
                               extra={"raw": True, "offset": 1.5, "dur": 7.0})
    assert make != miss and os.path.exists(make) and os.path.exists(miss)
    job = json.load(open(miss))
    assert job["log_id"] == "cv_17_left" and job["made"] is False
    assert job["raw"] is True and job["offset"] == 1.5 and job["dur"] == 7.0


class _Rec:
    def __init__(self, segs):
        self._segs = segs

    def segments(self, label=None):
        return self._segs

    def status(self):
        return {"label": "L"}

    def angle(self):
        return "FL"


class _Cfg:
    def __init__(self, d):
        self.output_dir = str(d)


@pytest.mark.unit
def test_tracker_cut_joins_the_buffer_by_stream_copy_and_queues_a_raw_job(tmp_path, monkeypatch):
    from agx_pipeline import highlight, tracker_queue
    now = 1_700_000_000.0
    segs = [(now - 12 + 4 * i, "FL", "/buf/seg%d.mp4" % i) for i in range(6)]     # covers now-12 .. now+12
    ran = []

    def fake_run(cmd, timeout=600):
        ran.append(cmd)
        open(cmd[-1], "wb").write(b"x")
        return True
    monkeypatch.setattr(highlight, "_run", fake_run)
    monkeypatch.setattr(tracker_queue, "QUEUE_DIR", str(tmp_path / "q"))
    p = highlight.cut_for_tracker(_Cfg(tmp_path), _Rec(segs), {"game_id": "G", "log_id": "cv_1700000000_left",
                                                              "ts_epoch": now, "pre": 5, "post": 2, "label": "L"})
    job = json.load(open(p))
    assert len(ran) == 1 and ran[0][ran[0].index("-c") + 1] == "copy"      # no decode, no encode
    assert job["raw"] is True and job["made"] is False and job["s3_key"] is None
    assert job["dur"] == 7.0 and 0.0 <= job["offset"] <= 4.0 and job["clip"].endswith("_FL_raw.mp4")


@pytest.mark.unit
def test_tracker_cut_never_raises_when_the_window_is_not_buffered(tmp_path):
    from agx_pipeline import highlight
    assert highlight.cut_for_tracker(_Cfg(tmp_path), _Rec([]), {"game_id": "G", "log_id": "x", "ts_epoch": 1.0,
                                                               "pre": 0.01, "post": 0.01, "label": "L"}) is None

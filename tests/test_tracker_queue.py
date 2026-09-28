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
